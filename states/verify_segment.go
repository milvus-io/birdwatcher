package states

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/minio/minio-go/v7"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
)

type VerifySegmentParam struct {
	framework.ParamBase `use:"verify-segment" desc:"Verify segment file matches storage"`

	CollectionID int64
	RootPath     string
	Fix          bool
}

func (s *InstanceState) VerifySegmentCommnad(ctx context.Context, p *VerifySegmentParam) error {
	fmt.Printf(`Using %s as storage rootPath, change by "--rootPath" flag if needed`, p.RootPath)

	segments, err := common.ListSegments(ctx, s.client, s.basePath, func(seg *models.Segment) bool {
		return seg.CollectionID == p.CollectionID && seg.State == commonpb.SegmentState_Flushed
	})
	if err != nil {
		fmt.Println("failed to list segment info", err.Error())
		return err
	}

	minioClient, bucketName, err := getMinioAccess()
	if err != nil {
		fmt.Println("failed to get minio access", err.Error())
		return err
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
			for _, fbl := range item.fieldBinlogs {
				for _, l := range fbl.Binlogs {
					logPath := strings.Replace(l.LogPath, "ROOT_PATH", p.RootPath, 1)
					_, err := minioClient.StatObject(ctx, bucketName, logPath, minio.StatObjectOptions{})
					if err != nil {
						errResp := minio.ToErrorResponse(err)
						fmt.Println("failed to check ", logPath, err, errResp.Code)
						if errResp.Code == "NoSuchKey" {
							switch item.tag {
							case "binlog":
								fmt.Println("path", logPath, p.Fix)
								splits := strings.Split(logPath, "/")
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

								if p.Fix && err == nil {
									err := common.RemoveSegmentInsertLogPath(ctx, s.client, s.basePath, collectionID, partitionID, segmentID, fieldID, logID)
									if err != nil {
										fmt.Println("failed to remove segment insert path")
									}
								}
							case "statslog":
								splits := strings.Split(logPath, "/")
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

								if p.Fix && err == nil {
									err := common.RemoveSegmentStatLogPath(ctx, s.client, s.basePath, collectionID, partitionID, segmentID, fieldID, logID)
									if err != nil {
										fmt.Println("failed to remove segment insert path")
									}
								}
							case "deltalog":
								splits := strings.Split(logPath, "/")
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

								if p.Fix && err == nil {
									err := common.RemoveSegmentDeltaLogPath(ctx, s.client, s.basePath, collectionID, partitionID, segmentID, logID)
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
	return nil
}
