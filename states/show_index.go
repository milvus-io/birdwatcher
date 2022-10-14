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
	indexpbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/indexpb"
	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/runtime/protoiface"
)

func listObject[T any, P interface {
	*T
	protoiface.MessageV1
}](ctx context.Context, cli *clientv3.Client, prefix string) ([]T, []string, error) {
	resp, err := cli.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, nil, err
	}
	result := make([]T, 0, len(resp.Kvs))
	keys := make([]string, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		var elem T
		info := P(&elem)
		err = proto.Unmarshal(kv.Value, info)
		if err != nil {
			fmt.Println(err.Error())
			continue
		}
		result = append(result, elem)
		keys = append(keys, string(kv.Key))
	}
	return result, keys, nil
}

func listSegmentIndex(cli *clientv3.Client, basePath string) ([]etcdpb.SegmentIndexInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	prefix := path.Join(basePath, "root-coord/segment-index") + "/"
	result, _, err := listObject[etcdpb.SegmentIndexInfo](ctx, cli, prefix)
	return result, err
}

func listSegmentIndexV2(cli *clientv3.Client, basePath string) ([]indexpbv2.SegmentIndex, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	prefix := path.Join(basePath, "segment-index") + "/"
	result, _, err := listObject[indexpbv2.SegmentIndex](ctx, cli, prefix)
	return result, err
}

func listIndex(cli *clientv3.Client, basePath string) ([]indexpb.IndexMeta, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	prefix := path.Join(basePath, "indexes") + "/"

	result, _, err := listObject[indexpb.IndexMeta](ctx, cli, prefix)
	return result, err
}

type IndexInfoV1 struct {
	info         etcdpb.IndexInfo
	collectionID int64
}

func listIndexMeta(cli *clientv3.Client, basePath string) ([]IndexInfoV1, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	prefix := path.Join(basePath, "root-coord/index")
	indexes, keys, err := listObject[etcdpb.IndexInfo](ctx, cli, prefix)
	result := make([]IndexInfoV1, 0, len(indexes))
	for idx, info := range indexes {
		collectionID, err := pathPartInt64(keys[idx], -2)
		if err != nil {
			continue
		}
		result = append(result, IndexInfoV1{
			info:         info,
			collectionID: collectionID,
		})
	}

	return result, err
}

func listIndexMetaV2(cli *clientv3.Client, basePath string) ([]indexpbv2.FieldIndex, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	indexes, _, err := listObject[indexpbv2.FieldIndex](ctx, cli, path.Join(basePath, "field-index"))
	return indexes, err
}

func getEtcdShowIndex(cli *clientv3.Client, basePath string) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "index",
		Aliases: []string{"indexes"},
		Run: func(cmd *cobra.Command, args []string) {

			// v2.0+
			meta, err := listIndexMeta(cli, basePath)
			if err != nil {
				fmt.Println(err.Error())
				return
			}

			for _, m := range meta {
				printIndex(m)
			}

			// v2.2+
			fieldIndexes, err := listIndexMetaV2(cli, basePath)
			if err != nil {
				fmt.Println(err.Error())
				return
			}

			for _, index := range fieldIndexes {
				printIndexV2(index)
			}
		},
	}
	return cmd
}

func printIndex(index IndexInfoV1) {
	fmt.Println("==================================================================")
	fmt.Printf("Index ID: %d\tIndex Name: %s\tCollectionID:%d\n", index.info.GetIndexID(), index.info.GetIndexName(), index.collectionID)
	indexParams := index.info.GetIndexParams()
	fmt.Printf("Index Type: %s\tMetric Type: %s\n",
		getKVPair(indexParams, "index_type"),
		getKVPair(indexParams, "metric_type"),
	)
	fmt.Printf("Index Params: %s\n", getKVPair(index.info.GetIndexParams(), "params"))
	fmt.Println("==================================================================")
}

func printIndexV2(index indexpbv2.FieldIndex) {
	fmt.Println("==================================================================")
	fmt.Printf("Index ID: %d\tIndex Name: %s\tCollectionID:%d\n", index.GetIndexInfo().GetIndexID(), index.GetIndexInfo().GetIndexName(), index.GetIndexInfo().GetCollectionID())
	createTime, _ := ParseTS(index.GetCreateTime())
	fmt.Printf("Create Time: %s\tDeleted: %t\n", createTime.Format(tsPrintFormat), index.GetDeleted())
	indexParams := index.GetIndexInfo().GetIndexParams()
	fmt.Printf("Index Type: %s\tMetric Type: %s\n",
		getKVPair(indexParams, "index_type"),
		getKVPair(indexParams, "metric_type"),
	)
	fmt.Printf("Index Params: %s\n", getKVPair(index.GetIndexInfo().GetUserIndexParams(), "params"))
	fmt.Println("==================================================================")
}

func getEtcdShowSegmentIndexCmd(cli *clientv3.Client, basePath string) *cobra.Command {
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
			segmentIndexesV2, err := listSegmentIndexV2(cli, basePath)
			if err != nil {
				fmt.Println(err.Error())
				return
			}

			indexBuildInfo, err := listIndex(cli, basePath)
			if err != nil {
				fmt.Println(err.Error())
				return
			}

			indexes, _, err := listObject[indexpbv2.FieldIndex](context.Background(), cli, path.Join(basePath, "field-index"))
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
							fmt.Printf("\t Index Type:%v on Field ID: %d", getKVPair(idx.GetIndexInfo().GetIndexParams(), "index_type"), idx.GetIndexInfo().GetFieldID())
						}
					}
					fmt.Println()
					continue
				}

				for _, segIdx := range segIdxs {
					info, ok := buildID2Info[segIdx.BuildID]
					if !ok {
						fmt.Printf("\tno build info found for id: %d\n", segIdx.BuildID)
						fmt.Println(segIdx.String())
					}
					fmt.Printf("\n\tIndex build ID: %d, state: %s", info.IndexBuildID, info.State.String())
					fmt.Printf("\t Index Type:%v on Field ID: %d", getKVPair(info.GetReq().GetIndexParams(), "index_type"), segIdx.GetFieldID())
				}
				fmt.Println()
			}

		},
	}

	cmd.Flags().Int64("collection", 0, "collection id to filter with")
	cmd.Flags().Int64("segment", 0, "segment id to filter with")
	return cmd
}
