package repair

import (
	"context"
	"fmt"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/birdwatcher/proto/v2.2/commonpb"
	"github.com/milvus-io/birdwatcher/proto/v2.2/datapb"
)

const (
	SegmentMetaPrefix = "datacoord-meta/s"
)

func writeRepairedSegmentV2(cli clientv3.KV, basePath string, segment *datapb.SegmentInfo) error {
	p := path.Join(basePath, fmt.Sprintf("datacoord-meta/s/%d/%d/%d", segment.CollectionID, segment.PartitionID, segment.ID))

	bs, err := proto.Marshal(segment)
	if err != nil {
		fmt.Println("failed to marshal segment info", err.Error())
	}
	_, err = cli.Put(context.Background(), p, string(bs))
	return err
}

// ListSegments list segment info from etcd based on datapb V2.2
func ListSegmentsV2(cli clientv3.KV, basePath string, filter func(*datapb.SegmentInfo) bool) ([]*datapb.SegmentInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	resp, err := cli.Get(ctx, path.Join(basePath, SegmentMetaPrefix)+"/", clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	segments := make([]*datapb.SegmentInfo, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		info := &datapb.SegmentInfo{}
		err = proto.Unmarshal(kv.Value, info)
		if err != nil {
			continue
		}
		if filter == nil || filter(info) {
			segments = append(segments, info)
		}
	}

	sort.Slice(segments, func(i, j int) bool {
		return segments[i].GetID() < segments[j].GetID()
	})
	return segments, nil
}

// JsonKeyStatsCommand return repair segment command.
func JsonKeyStatsCommand(cli clientv3.KV, basePath string) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "remove-json-stats",
		Aliases: []string{"remove-json-stats"},
		Short:   "remove json stats",
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

			fieldID, err := cmd.Flags().GetInt64("field")
			if err != nil {
				fmt.Println(err.Error())
				return
			}

			run, err := cmd.Flags().GetBool("run")
			if err != nil {
				fmt.Println(err.Error())
				return
			}

			if collID == 0 && segmentID == 0 {
				fmt.Println("collection id or segment id must be provided")
				return
			}

			segments, err := ListSegmentsV2(cli, basePath, func(info *datapb.SegmentInfo) bool {
				return (collID == 0 || info.CollectionID == collID) && (segmentID == 0 || info.ID == segmentID)
			})
			if err != nil {
				fmt.Println(err.Error())
				return
			}

			if !run {
				fmt.Println("display dry run details, if need to remove add --run true")
			}
			for _, segment := range segments {
				fmt.Println(" Segment ID: ", segment.ID)
				if segment.State != commonpb.SegmentState_Flushed {
					continue
				}

				for k, v := range segment.JsonKeyStats {
					if fieldID != 0 && k != fieldID {
						continue
					}
					fmt.Println("  field id: ", k)
					fmt.Println("   will remove json key stats files:", strings.Join(v.Files, ", "))
					if run {
						delete(segment.JsonKeyStats, k)
					}
				}
				if run {
					err := writeRepairedSegmentV2(cli, basePath, segment)
					if err != nil {
						fmt.Println("failed to write back repaired segment meta for segment id:", segment.ID)
					}

				}

			}
		},
	}
	cmd.Flags().Int64("collection", 0, "collection id to filter with")
	cmd.Flags().Int64("segment", 0, "segment id to filter with")
	cmd.Flags().Int64("field", 0, "field id to filter with")
	cmd.Flags().Bool("run", false, "whether to run the remove")
	return cmd
}
