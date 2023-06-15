package states

import (
	"context"
	"fmt"
	"os"
	"path"
	"strconv"

	"github.com/milvus-io/birdwatcher/proto/v2.0/commonpb"
	"github.com/milvus-io/birdwatcher/proto/v2.0/datapb"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	etcdversion "github.com/milvus-io/birdwatcher/states/etcd/version"
	"github.com/milvus-io/birdwatcher/storage"
	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func getInspectPKCmd(cli clientv3.KV, basePath string) *cobra.Command {

	cmd := &cobra.Command{
		Use:   "inspect-pk [segment id]",
		Short: "inspect pk num&dup condition",
		Run: func(cmd *cobra.Command, args []string) {
			var segments []*datapb.SegmentInfo
			var err error
			if len(args) == 0 {
				segments, err = common.ListSegments(cli, basePath, nil)
			} else {
				var segmentID int64
				segmentID, err = strconv.ParseInt(args[0], 10, 64)
				if err != nil {
					fmt.Println("failed to parse segment id")
					cmd.Help()
					return
				}
				segments, err = common.ListSegments(cli, basePath, func(info *datapb.SegmentInfo) bool { return info.ID == segmentID })
			}
			if err != nil {
				fmt.Println("failed to parse argument:", err.Error())
				return
			}

			// collid -> pk id
			cachedCollection := make(map[int64]int64)

			globalMap := make(map[int64]int64)

			for _, segment := range segments {
				if segment.State != commonpb.SegmentState_Flushed {
					continue
				}
				pkID, has := cachedCollection[segment.CollectionID]
				if !has {
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					coll, err := common.GetCollectionByIDVersion(ctx, cli, basePath, etcdversion.GetVersion(), segment.GetCollectionID())

					if err != nil {
						fmt.Println("Collection not found for id", segment.CollectionID)
						return
					}

					for _, field := range coll.Schema.Fields {
						if field.IsPrimaryKey {
							pkID = field.FieldID
							break
						}
					}

					if pkID < 0 {
						fmt.Println("collection pk not found")
						return
					}
				}

				common.FillFieldsIfV2(cli, basePath, segment)
				for _, fieldBinlog := range segment.Binlogs {
					if fieldBinlog.FieldID != pkID {
						continue
					}
					total := 0
					for _, binlog := range fieldBinlog.Binlogs {
						name := path.Base(binlog.GetLogPath())
						rp := fmt.Sprintf("%d/%d/%s", segment.CollectionID, segment.ID, name)
						f, err := os.Open(rp)
						if err != nil {
							fmt.Println(err)
							return
						}
						r, _, err := storage.NewBinlogReader(f)
						if err != nil {
							fmt.Println("fail to create binlog reader:", err.Error())
							continue
						}
						ids, err := r.NextEventReader(f)
						if err != nil {
							fmt.Println("faild to read next event:", err.Error())
							continue
						}
						f.Close()
						// binlog entries num = 0 skip
						if binlog.EntriesNum != int64(len(ids)) && binlog.EntriesNum > 0 {
							fmt.Printf("found mismatch segment id:%d, name:%s, binlog: %d, id-len%d\n", segment.ID, name, binlog.EntriesNum, len(ids))
						}

						/*
							c := ddup(ids)
							if c > 0 {
								fmt.Printf("found %d dup entry for segment %d-%s", c, segment.ID, name)
							}*/
						dr, c := globalDDup(segment.ID, ids, globalMap)
						if len(dr) > 0 || c > 0 {
							fmt.Printf("found %d dup entry for segment %d, distribution:%v\n", c, segment.ID, dr)
						}
						total += len(ids)
					}

					if int64(total) != segment.NumOfRows {
						fmt.Printf("found mismatch segment %d, info:%d, binlog count:%d", segment.ID, segment.NumOfRows, total)
					}
				}

			}

		},
	}

	return cmd
}

func ddup(ids []int64) int {
	m := make(map[int64]struct{})
	counter := 0
	for _, id := range ids {
		_, has := m[id]
		if has {
			counter++
		}
		m[id] = struct{}{}
	}
	return counter
}

func globalDDup(segmentID int64, ids []int64, m map[int64]int64) (map[int64]int, int) {
	count := 0
	dr := make(map[int64]int)
	for _, id := range ids {
		origin, has := m[id]
		if has {
			dr[origin]++
			count++
		} else {
			m[id] = segmentID
		}
	}
	return dr, count
}
