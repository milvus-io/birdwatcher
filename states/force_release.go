package states

import (
	"context"
	"fmt"
	"path"
	"time"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/proto/v2.0/querypb"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	etcdversion "github.com/milvus-io/birdwatcher/states/etcd/version"
	"github.com/milvus-io/birdwatcher/states/kv"
	"github.com/spf13/cobra"
)

type ForceReleaseParam struct {
	framework.ParamBase `use:"force-release"`
	CollectionID        int64 `name:"collection" default:"0" desc:"collection id to force release"`
	All                 bool  `name:"all" default:"false" desc:"force release all collections loaded"`
}

// getForceReleaseCmd returns command for force-release
// usage: force-release --collection [collection id]
// or force-release --all
func (s *InstanceState) ForceReleaseCommand(ctx context.Context, p *ForceReleaseParam) error {
	// release all collections / partitions
	if p.All {
		err := s.client.RemoveWithPrefix(ctx, "queryCoord-")
		if err != nil {
			fmt.Printf("failed to remove queryCoord v1 etcd kv, err: %v\n", err)
		}
		// remove all keys start with [basePath]/querycoord- qcv2 meta
		err = s.client.RemoveWithPrefix(ctx, "querycoord-")
		if err != nil {
			fmt.Printf("failed to remove queryCoord v2 etcd kv, err: %v\n", err)
		}
		return nil
	}

	collections, err := common.ListCollectionLoadedInfo(ctx, s.client, s.basePath, etcdversion.GetVersion(), func(cl *models.CollectionLoaded) bool {
		return cl.CollectionID == p.CollectionID
	})
	if err != nil {
		return err
	}
	partitions, err := common.ListPartitionLoadedInfo(ctx, s.client, s.basePath, etcdversion.GetVersion(), func(pl *models.PartitionLoaded) bool {
		return p.CollectionID == pl.CollectionID
	})
	if err != nil {
		return err
	}
	if len(collections) == 0 && len(partitions) == 0 {
		fmt.Println("no collections/partitions selected")
		return nil
	}

	for _, cl := range collections {
		fmt.Printf("Force release collection %d, key: %s\n", cl.CollectionID, cl.Key)
		err := s.client.Remove(ctx, cl.Key)
		if err != nil {
			fmt.Printf("failed to force release collection %d, err: %v\n", cl.CollectionID, err)
			continue
		}
		fmt.Printf("Force release collection %d done", cl.CollectionID)
	}
	for _, pl := range partitions {
		fmt.Printf("Force release partition %d, key: %s\n", pl.PartitionID, pl.Key)
		err := s.client.Remove(ctx, pl.Key)
		if err != nil {
			fmt.Printf("failed to force release partition %d, err: %v\n", pl.PartitionID, err)
			continue
		}
		fmt.Printf("Force release partition %d done", pl.PartitionID)
	}
	return nil
}

// getReleaseDroppedCollectionCmd returns command for release-dropped-collection
func getReleaseDroppedCollectionCmd(cli kv.MetaKV, basePath string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "release-dropped-collection",
		Short: "Clean loaded collections meta if it's dropped from QueryCoord",
		Run: func(cmd *cobra.Command, args []string) {
			if etcdversion.GetVersion() != models.LTEVersion2_1 {
				fmt.Println("force-release only for Milvus version <= 2.1.4")
				return
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			// force release only for version <= v2.1.4
			collectionLoadInfos, err := common.ListCollectionLoadedInfo(ctx, cli, basePath, models.LTEVersion2_1)
			if err != nil {
				fmt.Println("failed to list loaded collections", err.Error())
				return
			}

			var missing []int64
			for _, info := range collectionLoadInfos {
				_, err := common.GetCollectionByIDVersion(ctx, cli, basePath, etcdversion.GetVersion(), info.CollectionID)
				if err != nil {
					missing = append(missing, info.CollectionID)
				}
			}
			for _, id := range missing {
				fmt.Printf("Collection %d is missing\n", id)
			}
			run, err := cmd.Flags().GetBool("run")
			if err == nil && run {

				for _, id := range missing {
					fmt.Printf("Start to remove loaded meta from querycoord, collection id %d...", id)
					err := releaseQueryCoordLoadMeta(cli, basePath, id)
					if err != nil {
						fmt.Println("failed, err:", err.Error())
					} else {
						fmt.Println(" done.")
					}
				}
			}
		},
	}

	cmd.Flags().Bool("run", false, "flags indicating whether to remove load collection info from meta")
	return cmd
}

func releaseQueryCoordLoadMeta(cli kv.MetaKV, basePath string, collectionID int64) error {
	p := path.Join(basePath, common.CollectionLoadPrefix, fmt.Sprintf("%d", collectionID))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	err := cli.Remove(ctx, p)

	if err != nil {
		return err
	}

	segments, err := common.ListLoadedSegments(cli, basePath, func(info *querypb.SegmentInfo) bool {
		return info.CollectionID == collectionID
	})

	if err != nil {
		return err
	}

	for _, segment := range segments {
		p := path.Join(basePath, "queryCoord-segmentMeta", fmt.Sprintf("%d/%d/%d", segment.CollectionID, segment.PartitionID, segment.SegmentID))
		cli.Remove(ctx, p)
	}

	dmChannels, err := common.ListQueryCoordDMLChannelInfos(cli, basePath)
	if err != nil {
		return err
	}

	for _, dmChannel := range dmChannels {
		if dmChannel.CollectionID != collectionID {
			continue
		}
		p := path.Join(basePath, common.QCDmChannelMetaPrefix, fmt.Sprintf("%d/%s", dmChannel.CollectionID, dmChannel.DmChannel))
		cli.Remove(ctx, p)
	}

	deltaChannels, err := common.ListQueryCoordDeltaChannelInfos(cli, basePath)
	for _, deltaChannel := range deltaChannels {
		if deltaChannel.CollectionID != collectionID {
			continue
		}
		p := path.Join(basePath, common.QCDeltaChannelMetaPrefix, fmt.Sprintf("%d/%s", deltaChannel.CollectionID, deltaChannel.ChannelName))
		cli.Remove(ctx, p)
	}

	return err
}
