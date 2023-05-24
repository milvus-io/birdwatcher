package show

import (
	"context"
	"fmt"
	"path"
	"time"

	"github.com/milvus-io/birdwatcher/proto/v2.0/commonpb"
	"github.com/milvus-io/birdwatcher/proto/v2.0/datapb"
	"github.com/milvus-io/birdwatcher/proto/v2.0/etcdpb"
	"github.com/milvus-io/birdwatcher/proto/v2.0/indexpb"
	indexpbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/indexpb"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// SegmentIndexCommand returns show segment-index command.
func SegmentIndexCommand(cli clientv3.KV, basePath string) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "segment-index",
		Aliases: []string{"segments-index", "segment-indexes", "segments-indexes"},
		Short:   "display segment index information",
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

			segments, err := common.ListSegments(cli, basePath, func(info *datapb.SegmentInfo) bool {
				return (collID == 0 || info.CollectionID == collID) &&
					(segmentID == 0 || info.ID == segmentID)
			})
			if err != nil {
				fmt.Println(err.Error())
				return
			}

			segmentIndexes, err := common.ListSegmentIndex(cli, basePath)
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			segmentIndexesV2, err := listSegmentIndexV2(cli, basePath)
			if err != nil {
				fmt.Println(err.Error())
				return
			}

			indexBuildInfo, err := common.ListIndex(cli, basePath)
			if err != nil {
				fmt.Println(err.Error())
				return
			}

			indexes, _, err := common.ListProtoObjects[indexpbv2.FieldIndex](context.Background(), cli, path.Join(basePath, "field-index"))
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			idIdx := make(map[int64]indexpbv2.FieldIndex)
			for _, idx := range indexes {
				idIdx[idx.IndexInfo.IndexID] = idx
			}

			seg2Idx := make(map[int64][]etcdpb.SegmentIndexInfo)
			seg2Idxv2 := make(map[int64][]indexpbv2.SegmentIndex)
			for _, segIdx := range segmentIndexes {
				idxs, ok := seg2Idx[segIdx.SegmentID]
				if !ok {
					idxs = []etcdpb.SegmentIndexInfo{}
				}

				idxs = append(idxs, segIdx)

				seg2Idx[segIdx.GetSegmentID()] = idxs
			}
			for _, segIdx := range segmentIndexesV2 {
				idxs, ok := seg2Idxv2[segIdx.SegmentID]
				if !ok {
					idxs = []indexpbv2.SegmentIndex{}
				}

				idxs = append(idxs, segIdx)

				seg2Idxv2[segIdx.GetSegmentID()] = idxs
			}

			buildID2Info := make(map[int64]indexpb.IndexMeta)
			for _, info := range indexBuildInfo {
				buildID2Info[info.IndexBuildID] = info
			}

			for _, segment := range segments {
				if segment.State != commonpb.SegmentState_Flushed {
					continue
				}
				fmt.Printf("SegmentID: %d\t State: %s", segment.GetID(), segment.GetState().String())
				segIdxs, ok := seg2Idx[segment.GetID()]
				if !ok {
					// try v2 index information
					segIdxv2, ok := seg2Idxv2[segment.GetID()]
					if !ok {
						fmt.Println("\tno segment index info")
						continue
					}
					for _, segIdx := range segIdxv2 {
						fmt.Printf("\n\tIndexV2 build ID: %d, states %s", segIdx.GetBuildID(), segIdx.GetState().String())
						idx, ok := idIdx[segIdx.GetIndexID()]
						if ok {
							fmt.Printf("\t Index Type:%v on Field ID: %d", common.GetKVPair(idx.GetIndexInfo().GetIndexParams(), "index_type"), idx.GetIndexInfo().GetFieldID())
						}
						fmt.Printf("\tSerialized Size: %d\n", segIdx.GetSerializeSize())
					}
					fmt.Println()
				}

				for _, segIdx := range segIdxs {
					info, ok := buildID2Info[segIdx.BuildID]
					if !ok {
						fmt.Printf("\tno build info found for id: %d\n", segIdx.BuildID)
						fmt.Println(segIdx.String())
					}
					fmt.Printf("\n\tIndex build ID: %d, state: %s", info.IndexBuildID, info.State.String())
					fmt.Printf("\t Index Type:%v on Field ID: %d", common.GetKVPair(info.GetReq().GetIndexParams(), "index_type"), segIdx.GetFieldID())
					fmt.Printf("\t info.SerializeSize: %d\n", info.GetSerializeSize())
				}
				fmt.Println()
			}

		},
	}

	cmd.Flags().Int64("collection", 0, "collection id to filter with")
	cmd.Flags().Int64("segment", 0, "segment id to filter with")
	return cmd
}

func listSegmentIndexV2(cli clientv3.KV, basePath string) ([]indexpbv2.SegmentIndex, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	prefix := path.Join(basePath, "segment-index") + "/"
	result, _, err := common.ListProtoObjects[indexpbv2.SegmentIndex](ctx, cli, prefix)
	return result, err
}
