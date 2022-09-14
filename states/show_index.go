package states

import (
	"context"
	"fmt"
	"path"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/birdwatcher/proto/v2.0/commonpb"
	"github.com/milvus-io/birdwatcher/proto/v2.0/datapb"
	"github.com/milvus-io/birdwatcher/proto/v2.0/etcdpb"
	"github.com/milvus-io/birdwatcher/proto/v2.0/indexpb"
	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/runtime/protoiface"
)

func listObject[T any, P interface {
	*T
	protoiface.MessageV1
}](ctx context.Context, cli *clientv3.Client, prefix string) ([]T, error) {
	resp, err := cli.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	result := make([]T, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		var elem T
		info := P(&elem)
		err = proto.Unmarshal(kv.Value, info)
		if err != nil {
			fmt.Println(err.Error())
			continue
		}
		result = append(result, elem)
	}
	return result, nil
}

func listSegmentIndex(cli *clientv3.Client, basePath string) ([]etcdpb.SegmentIndexInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	prefix := path.Join(basePath, "root-coord/segment-index") + "/"
	return listObject[etcdpb.SegmentIndexInfo](ctx, cli, prefix)
}

func listIndex(cli *clientv3.Client, basePath string) ([]indexpb.IndexMeta, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	prefix := path.Join(basePath, "indexes") + "/"

	return listObject[indexpb.IndexMeta](ctx, cli, prefix)
}

func getEtcdShowIndex(cli *clientv3.Client, basePath string) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "index",
		Aliases: []string{"indexes"},
		Run: func(cmd *cobra.Command, args []string) {
			indexes, err := listSegmentIndex(cli, basePath)
			if err != nil {
				fmt.Println(err.Error())
				return
			}

			for _, index := range indexes {
				fmt.Println(index.String())
			}

			meta, err := listIndex(cli, basePath)
			if err != nil {
				fmt.Println(err.Error())
				return
			}

			for _, m := range meta {
				fmt.Println(m.String())
			}
		},
	}
	return cmd
}

func getEtcdShowSegmentIndexCmd(cli *clientv3.Client, basePath string) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "segment-index",
		Aliases: []string{"segments-index", "segment-indexes", "segments-indexes"},
		Run: func(cmd *cobra.Command, args []string) {
			collID, err := cmd.Flags().GetInt64("collection")
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			segmentID, err := cmd.Flags().GetInt64("segment")
			if err != nil {
				fmt.Println(err.Error())
				return
			}

			segments, err := listSegments(cli, basePath, func(info *datapb.SegmentInfo) bool {
				return (collID == 0 || info.CollectionID == collID) &&
					(segmentID == 0 || info.ID == segmentID)
			})
			if err != nil {
				fmt.Println(err.Error())
				return
			}

			segmentIndexes, err := listSegmentIndex(cli, basePath)
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			if err != nil {
				fmt.Println(err.Error())
				return
			}

			indexBuildInfo, err := listIndex(cli, basePath)
			if err != nil {
				fmt.Println(err.Error())
				return
			}

			seg2Idx := make(map[int64]etcdpb.SegmentIndexInfo)
			for _, segIdx := range segmentIndexes {
				seg2Idx[segIdx.GetSegmentID()] = segIdx
			}

			buildID2Info := make(map[int64]indexpb.IndexMeta)
			for _, info := range indexBuildInfo {
				buildID2Info[info.IndexBuildID] = info
			}

			for _, segment := range segments {
				fmt.Printf("SegmentID: %d\t State: %s", segment.GetID(), segment.GetState().String())
				if segment.State != commonpb.SegmentState_Flushed {
					fmt.Println()
					continue
				}
				segIdx, ok := seg2Idx[segment.GetID()]
				if !ok {
					fmt.Println("\tno segment index info")
					continue
				}
				info, ok := buildID2Info[segIdx.BuildID]
				if !ok {
					fmt.Printf("\tno build info found for id: %d\n", segIdx.BuildID)
				}
				fmt.Printf("\tIndex build ID: %d, state: %s \n", info.IndexBuildID, info.State.String())
			}

		},
	}

	cmd.Flags().Int64("collection", 0, "collection id to filter with")
	cmd.Flags().Int64("segment", 0, "segment id to filter with")
	return cmd
}
