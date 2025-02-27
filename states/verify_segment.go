package states

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/spf13/cobra"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/birdwatcher/states/kv"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
)

func getVerifySegmentCmd(cli kv.MetaKV, basePath string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "verify-segment",
		Short: "Verify segment file matches storage",
		Run: func(cmd *cobra.Command, args []string) {
			collectionID, err := cmd.Flags().GetInt64("collection")
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			fix, err := cmd.Flags().GetBool("fix")
			if err != nil {
				fmt.Println(err.Error())
				return
			}

			segments, err := common.ListSegments(context.Background(), cli, basePath, func(seg *models.Segment) bool {
				return seg.CollectionID == collectionID && seg.State == commonpb.SegmentState_Flushed
			})
			if err != nil {
				fmt.Println("failed to list segment info", err.Error())
			}

			minioClient, bucketName, err := getMinioAccess()
			if err != nil {
				fmt.Println("failed to get minio access", err.Error())
				return
			}

			total := len(segments)
			for idx, segment := range segments {
				fmt.Printf("Start to verify segment(%d) --- (%d/%d)\n", segment.ID, idx, total)
				type item struct {
					tag          string
					fieldBinlogs []*models.FieldBinlog
				}
				items := []item{
					{tag: "binlog", fieldBinlogs: segment.GetBinlogs()},
					{tag: "statslog", fieldBinlogs: segment.GetStatslogs()},
					{tag: "deltalog", fieldBinlogs: segment.GetDeltalogs()},
				}
				for _, item := range items {
					for _, statslog := range item.fieldBinlogs {
						for _, l := range statslog.Binlogs {
							_, err := minioClient.StatObject(context.Background(), bucketName, l.LogPath, minio.StatObjectOptions{})
							if err != nil {
								errResp := minio.ToErrorResponse(err)
								fmt.Println("failed to check ", l.LogPath, err, errResp.Code)
								if errResp.Code == "NoSuchKey" {
									if item.tag == "binlog" {
										fmt.Println("path", l.LogPath, fix)
										splits := strings.Split(l.LogPath, "/")
										logID, err := strconv.ParseInt(splits[len(splits)-1], 10, 64)
										if err != nil {
											fmt.Println("failed to parse logID")
										}

										fieldID, err := strconv.ParseInt(splits[len(splits)-2], 10, 64)
										if err != nil {
											fmt.Println("failed to parse fieldID")
										}

										segmentID, err := strconv.ParseInt(splits[len(splits)-3], 10, 64)
										if err != nil {
											fmt.Println("failed to parse segmentID")
										}

										partitionID, err := strconv.ParseInt(splits[len(splits)-4], 10, 64)
										if err != nil {
											fmt.Println("failed to parse partitionID")
										}

										collectionID, err := strconv.ParseInt(splits[len(splits)-5], 10, 64)
										if err != nil {
											fmt.Println("failed to parse collectionID")
										}

										if fix && err == nil {
											err := common.RemoveSegmentInsertLogPath(context.Background(), cli, basePath, collectionID, partitionID, segmentID, fieldID, logID)
											if err != nil {
												fmt.Println("failed to remove segment insert path")
											}
										}
									} else if item.tag == "statslog" {
										splits := strings.Split(l.LogPath, "/")
										logID, err := strconv.ParseInt(splits[len(splits)-1], 10, 64)
										if err != nil {
											fmt.Println("failed to parse logID")
										}

										fieldID, err := strconv.ParseInt(splits[len(splits)-2], 10, 64)
										if err != nil {
											fmt.Println("failed to parse fieldID")
										}

										segmentID, err := strconv.ParseInt(splits[len(splits)-3], 10, 64)
										if err != nil {
											fmt.Println("failed to parse segmentID")
										}

										partitionID, err := strconv.ParseInt(splits[len(splits)-4], 10, 64)
										if err != nil {
											fmt.Println("failed to parse parititonID")
										}

										collectionID, err := strconv.ParseInt(splits[len(splits)-5], 10, 64)
										if err != nil {
											fmt.Println("failed to parse col id")
										}

										if fix && err == nil {
											err := common.RemoveSegmentStatLogPath(context.Background(), cli, basePath, collectionID, partitionID, segmentID, fieldID, logID)
											if err != nil {
												fmt.Println("failed to remove segment insert path")
											}
										}
									} else if item.tag == "deltalog" {
										splits := strings.Split(l.LogPath, "/")
										logID, err := strconv.ParseInt(splits[len(splits)-1], 10, 64)
										if err != nil {
											fmt.Println("failed to parse logID")
										}
										segmentID, err := strconv.ParseInt(splits[len(splits)-2], 10, 64)
										if err != nil {
											fmt.Println("failed to parse segmentID")
										}

										partitionID, err := strconv.ParseInt(splits[len(splits)-3], 10, 64)
										if err != nil {
											fmt.Println("failed to parse partitionID")
										}

										collectionID, err := strconv.ParseInt(splits[len(splits)-4], 10, 64)
										if err != nil {
											fmt.Println("failed to parse col id")
										}

										if fix && err == nil {
											err := common.RemoveSegmentDeltaLogPath(context.Background(), cli, basePath, collectionID, partitionID, segmentID, logID)
											if err != nil {
												fmt.Println("failed to remove segment insert path")
											}
										}
									}
								}
							}
						}
					}

					fmt.Printf("Segment(%d) %s done\n", segment.ID, item.tag)
				}
			}
		},
	}

	cmd.Flags().Int64("collection", 0, "collection id")
	cmd.Flags().Bool("fix", false, "remove the log path to fix no such key")
	return cmd
}
