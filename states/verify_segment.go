package states

import (
	"context"
	"fmt"
	"strings"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	etcdversion "github.com/milvus-io/birdwatcher/states/etcd/version"
	"github.com/minio/minio-go/v7"
	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func getVerifySegmentCmd(cli clientv3.KV, basePath string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "verify-segment",
		Short: "Verify segment file matches storage",
		Run: func(cmd *cobra.Command, args []string) {
			collectionID, err := cmd.Flags().GetInt64("collection")
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			patch, err := cmd.Flags().GetBool("patch")
			if err != nil {
				fmt.Println(err.Error())
				return
			}

			segments, err := common.ListSegmentsVersion(context.Background(), cli, basePath, etcdversion.GetVersion(), func(seg *models.Segment) bool {
				return seg.CollectionID == collectionID && seg.State == models.SegmentStateFlushed
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
								if errResp.Code != "NoSuchKey" {
									fmt.Println("failed to stat object in minio", err.Error())
									continue
								}
								if !patch {
									fmt.Println("file not exists in minio", l.LogPath)
									continue
								}
								// try to patch 01 => 1 bug
								if item.tag == "statslog" && strings.HasSuffix(l.LogPath, "/1") {
									currentObjectPath := strings.TrimSuffix(l.LogPath, "/1") + "/01"
									_, err = minioClient.StatObject(context.Background(), bucketName, currentObjectPath, minio.StatObjectOptions{})
									if err != nil {
										fmt.Println(currentObjectPath, "also not exists")
										continue
									}
									fmt.Printf("current statslog(%s) for (%s) found, try to copy object", currentObjectPath, l.LogPath)
									minioClient.CopyObject(context.Background(), minio.CopyDestOptions{
										Bucket: bucketName,
										Object: l.LogPath,
									}, minio.CopySrcOptions{
										Bucket: bucketName,
										Object: currentObjectPath,
									})
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
	cmd.Flags().Bool("patch", false, "try to patch with known issue logic")
	return cmd
}
