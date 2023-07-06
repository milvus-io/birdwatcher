package repair

import (
	"context"
	"fmt"
	"path"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/proto/v2.0/commonpb"
	"github.com/milvus-io/birdwatcher/proto/v2.0/datapb"
	"github.com/milvus-io/birdwatcher/proto/v2.0/etcdpb"
	"github.com/milvus-io/birdwatcher/proto/v2.0/indexpb"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	etcdversion "github.com/milvus-io/birdwatcher/states/etcd/version"
	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// SegmentCommand return repair segment command.
func SegmentCommand(cli clientv3.KV, basePath string) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "segment",
		Aliases: []string{"segments"},
		Short:   "do segment & index meta check and try to repair",
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
			/*
				skipDownload, err := cmd.Flags().GetBool("skip-download")
				if err != nil {
					fmt.Println(err.Error())
					return
				}*/
			run, err := cmd.Flags().GetBool("run")
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

			// use v1 meta for now
			segmentIndexes, err := common.ListSegmentIndex(cli, basePath)
			if err != nil {
				fmt.Println(err.Error())
				return
			}

			indexBuildInfo, err := common.ListIndex(context.Background(), cli, basePath)
			if err != nil {
				fmt.Println(err.Error())
				return
			}

			seg2Idx := make(map[int64][]etcdpb.SegmentIndexInfo)

			for _, segIdx := range segmentIndexes {
				idxs, ok := seg2Idx[segIdx.SegmentID]
				if !ok {
					idxs = []etcdpb.SegmentIndexInfo{}
				}

				idxs = append(idxs, segIdx)

				seg2Idx[segIdx.GetSegmentID()] = idxs
			}

			buildID2Info := make(map[int64]indexpb.IndexMeta)
			for _, info := range indexBuildInfo {
				buildID2Info[info.IndexBuildID] = info
			}

			collections := make(map[int64]*models.Collection)

			targetOld := make(map[int64]*datapb.SegmentInfo)
			target := make(map[int64]*datapb.SegmentInfo)
			targetIndex := make(map[int64]*indexpb.IndexMeta)

			for _, segment := range segments {
				if segment.State != commonpb.SegmentState_Flushed {
					continue
				}
				segIdxs, ok := seg2Idx[segment.GetID()]
				if !ok {
					// skip index check for no index found
					//TODO try v2 index information
					continue
				}

				coll, ok := collections[segment.CollectionID]
				if !ok {
					//coll, err = common.GetCollectionByID(cli, basePath, segment.CollectionID)
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					coll, err := common.GetCollectionByIDVersion(ctx, cli, basePath, etcdversion.GetVersion(), collID)

					if err != nil {
						fmt.Printf("failed to query collection(id=%d) info error: %s", segment.CollectionID, err.Error())
						continue
					}
					collections[segment.CollectionID] = coll
				}

				common.FillFieldsIfV2(cli, basePath, segment)

				for _, segIdx := range segIdxs {
					var valid bool
					for _, field := range coll.Schema.Fields {
						if field.FieldID == segIdx.GetFieldID() {
							if field.DataType == models.DataTypeFloatVector || field.DataType == models.DataTypeBinaryVector {
								valid = true
							}
							break
						}
					}
					if !valid {
						// skip index if not found or not Vector index
						continue
					}
					info, ok := buildID2Info[segIdx.BuildID]
					if !ok {
						// skip index check for no index found
						continue
					}
					updated, found := checkBinlogIndex(segment, &info)
					if found {
						target[segment.ID] = updated
						targetIndex[segment.ID] = &info
						targetOld[segment.ID] = segment
					}
				}
			}
			if len(target) == 0 {
				fmt.Println("no error found")
				return
			}
			if !run {
				return
			}

			/*
				if !skipDownload {
					minioClient, bucketName, err := getMinioAccess()

					if err != nil {
						fmt.Println("failed to get minio access", err.Error())
					}

					folder := fmt.Sprintf("repair_segment_%s", time.Now().Format("20060102150406"))
					for segmentID, segment := range target {
						fileName := fmt.Sprintf("segment_%d", segmentID)
						oldSeg := targetOld[segmentID]
						f, err := os.Create(path.Join(folder, fileName))
						if err != nil {
							fmt.Println("failed to open file", fileName, err.Error())
							return
						}
						bs, err := proto.Marshal(oldSeg)
						w := bufio.NewWriter(f)
						w.Write(bs)
						w.Flush()
						f.Close()

						index, ok := targetIndex[segmentID]
						if ok {
							fileName = fmt.Sprintf("index_%d", segmentID)
							f, err = os.Create(path.Join(folder, fileName))
							if err != nil {
								fmt.Println("failed to open file", fileName, err.Error())
								return
							}
							bs, err = proto.Marshal(index)
							w := bufio.NewWriter(f)
							w.Write(bs)
							w.Flush()
							f.Close()

						}

						err = downloadSegment(minioClient, bucketName, segment, targetIndex[segmentID], folder)
						if err != nil {
							fmt.Println("failed to download segment", err.Error())
							return
						}
					}
				}*/

			// row count check
			for segmentID, segment := range target {
				if !integrityCheck(segment) {
					fmt.Printf("generated segment[id=%d] row count not match, repair failed.", segmentID)
					delete(target, segmentID)
				}
			}

			// update segment meta, v1 format for now
			for _, segment := range target {
				err := writeRepairedSegment(cli, basePath, segment)
				if err != nil {
					fmt.Println("failed to write back repaired segment meta")
				}
			}

		},
	}

	cmd.Flags().Int64("collection", 0, "collection id to filter with")
	cmd.Flags().Int64("segment", 0, "segment id to filter with")
	cmd.Flags().Bool("skip-download", false, "skip download segment files")
	cmd.Flags().Bool("run", false, "actual do repair")
	return cmd
}

func checkBinlogIndex(segment *datapb.SegmentInfo, indexMeta *indexpb.IndexMeta) (*datapb.SegmentInfo, bool) {
	indexed := make(map[string]struct{})
	for _, path := range indexMeta.GetReq().GetDataPaths() {
		indexed[path] = struct{}{}
	}

	excludedIdx := make(map[int]struct{})

	for _, fieldLog := range segment.GetBinlogs() {
		if fieldLog.FieldID == indexMeta.GetReq().GetFieldSchema().GetFieldID() {
			for idx, binlog := range fieldLog.GetBinlogs() {
				_, ok := indexed[binlog.GetLogPath()]
				if !ok {
					excludedIdx[idx] = struct{}{}
					fmt.Printf("[segment=%d]binlog %s in segment but not indexed\n", segment.ID, binlog.LogPath)
				}
			}
		}
	}

	if len(excludedIdx) == 0 {
		return nil, false
	}

	// remove binlog from field logs
	update := proto.Clone(segment).(*datapb.SegmentInfo)
	for _, fieldLog := range update.GetBinlogs() {
		newbinlogs := make([]*datapb.Binlog, 0, len(fieldLog.GetBinlogs()))
		for idx, binlog := range fieldLog.GetBinlogs() {
			_, exclude := excludedIdx[idx]
			if !exclude {
				newbinlogs = append(newbinlogs, binlog)
			}
		}
		fieldLog.Binlogs = newbinlogs
	}

	return update, true
}

func integrityCheck(segment *datapb.SegmentInfo) bool {
	var rowCount int64
	//use 0-th field as base
	for _, binlog := range segment.GetBinlogs()[0].GetBinlogs() {
		rowCount += binlog.EntriesNum
	}

	for i := 1; i < len(segment.GetBinlogs()); i++ {
		var frc int64
		for _, binlog := range segment.GetBinlogs()[i].GetBinlogs() {
			frc += binlog.EntriesNum
		}

		if frc != rowCount {
			return false
		}
	}

	return true
}

func writeRepairedSegment(cli clientv3.KV, basePath string, segment *datapb.SegmentInfo) error {
	p := path.Join(basePath, fmt.Sprintf("datacoord-meta/s/%d/%d/%d", segment.CollectionID, segment.PartitionID, segment.ID))

	bs, err := proto.Marshal(segment)
	if err != nil {
		fmt.Println("failed to marshal segment info", err.Error())
	}
	_, err = cli.Put(context.Background(), p, string(bs))
	return err

}
