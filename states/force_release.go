package states

import (
	"context"
	"fmt"
	"path"
	"time"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/proto/v2.0/querypb"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	etcdversion "github.com/milvus-io/birdwatcher/states/etcd/version"
	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// getForceReleaseCmd returns command for force-release
// usage: force-release [flags]
func getForceReleaseCmd(cli clientv3.KV, basePath string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "force-release",
		Short: "Force release the collections from QueryCoord",
		Run: func(cmd *cobra.Command, args []string) {
			/*
				// basePath = 'by-dev/meta/'
				// queryCoord prefix = 'queryCoord-'
				now := time.Now()
				err := backupEtcd(cli, basePath, "queryCoord-", string(compQueryCoord), fmt.Sprintf("bw_etcd_querycoord.%s.bak.gz", now.Format("060102-150405")), false)
				if err != nil {
					fmt.Printf("backup etcd failed, error: %v, stop doing force-release\n", err)
				}*/

			// remove all keys start with [basePath]/queryCoord- qcv1 meta
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()
			_, err := cli.Delete(ctx, path.Join(basePath, "queryCoord-"), clientv3.WithPrefix())
			if err != nil {
				fmt.Printf("failed to remove queryCoord v1 etcd kv, err: %v\n", err)
			}
			// remove all keys start with [basePath]/querycoord- qcv2 meta
			_, err = cli.Delete(ctx, path.Join(basePath, "querycoord-"), clientv3.WithPrefix())
			if err != nil {
				fmt.Printf("failed to remove queryCoord v2 etcd kv, err: %v\n", err)
			}

			// release all collections from online querynodes
			// maybe? kill session of queryCoord?
		},
	}

	return cmd
}

// getReleaseDroppedCollectionCmd returns command for release-dropped-collection
func getReleaseDroppedCollectionCmd(cli clientv3.KV, basePath string) *cobra.Command {
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
				_, err := common.GetCollectionByID(cli, basePath, info.CollectionID)
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

func releaseQueryCoordLoadMeta(cli clientv3.KV, basePath string, collectionID int64) error {
	p := path.Join(basePath, common.CollectionLoadPrefix, fmt.Sprintf("%d", collectionID))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	_, err := cli.Delete(ctx, p)

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
		cli.Delete(ctx, p)
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
		cli.Delete(ctx, p)
	}

	deltaChannels, err := common.ListQueryCoordDeltaChannelInfos(cli, basePath)
	for _, deltaChannel := range deltaChannels {
		if deltaChannel.CollectionID != collectionID {
			continue
		}
		p := path.Join(basePath, common.QCDeltaChannelMetaPrefix, fmt.Sprintf("%d/%s", deltaChannel.CollectionID, deltaChannel.ChannelName))
		cli.Delete(ctx, p)
	}

	return err
}
