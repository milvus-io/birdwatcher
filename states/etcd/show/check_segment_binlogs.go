package show

import (
	"context"
	"fmt"
	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	etcdversion "github.com/milvus-io/birdwatcher/states/etcd/version"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio-go/v7"
)

type CheckSegmentBinlogParam struct {
	framework.ParamBase `use:"check segment binlog" desc:"check segment binlods exists in minio" alias:"segments"`
	CollectionID        int64  `name:"collection" default:"0" desc:"collection id to filter with"`
	PartitionID         int64  `name:"partition" default:"0" desc:"partition id to filter with"`
	Detail              bool   `name:"detail" default:"false" desc:"flags indicating whether printing detail binlog info"`
	S3Address           string `name:"minio" default:"localhost:9000" desc:"flags for minio address"`
	BucketName          string `name:"bucket" default:"a-bucket" desc:"bucket name for minio"`
	AccessKeyID         string `name:"accessKeyID" default:"minioadmin" desc:"accessKeyID for minio"`
	SecretAccessKeyID   string `name:"secretAccessKeyID" default:"minioadmin" desc:"secretAccessKeyID for minio"`
	RootPath            string `name:"minioRootPath" default:"files" desc:"secretAccessKeyID for minio"`
	UseSSL              bool   `name:"useSSL" default:"false" desc:"secretAccessKeyID for minio"`
}

// CheckSegmentBinlogCommand returns check segments binlog exists.
func (c *ComponentShow) CheckSegmentBinlogCommand(ctx context.Context, p *CheckSegmentBinlogParam) error {
	segments, err := common.ListSegmentsVersion(ctx, c.client, c.basePath, etcdversion.GetVersion(), func(segment *models.Segment) bool {
		return (p.CollectionID == 0 || segment.CollectionID == p.CollectionID) &&
			(p.PartitionID == 0 || segment.PartitionID == p.PartitionID) &&
			segment.State != models.SegmentStateDropped
	})
	if err != nil {
		fmt.Println("failed to list segments", err.Error())
		return nil
	}

	minioClient, err := minio.New(p.S3Address, &minio.Options{
		Creds:  credentials.NewStaticV4(p.AccessKeyID, p.SecretAccessKeyID, ""),
		Secure: p.UseSSL,
	})
	if err != nil {
		fmt.Printf("create minio client failed: err = %v", err)
		return err
	}

	bucketName := p.BucketName
	bucketExists, err := minioClient.BucketExists(ctx, p.BucketName)
	if err != nil {
		fmt.Printf("check bucket: %s exist failed: %v\n", p.BucketName, err)
		return err
	}
	if !bucketExists {
		fmt.Printf("bucket: %s not exist, please check it\n", p.BucketName)
		return nil
	}
	fmt.Printf("segments num: %d\n", len(segments))

	invalidSegments := 0
	lostRows := int64(0)
	for _, segment := range segments {
		isInvalid := false
		for _, binlog := range segment.GetBinlogs() {
			for _, l := range binlog.Binlogs {
				// objectName := fmt.Sprintf("%s/insert_log/%d/%d/%d/%d/%d", p.RootPath, segment.CollectionID, segment.PartitionID, segment.ID, binlog.FieldID, l.LogPath)
				objectName := l.LogPath
				_, err = minioClient.StatObject(context.Background(), bucketName, objectName, minio.StatObjectOptions{})
				if err != nil {
					errResponse := minio.ToErrorResponse(err)
					if errResponse.Code == "NoSuchKey" {
						//fmt.Printf("segment: %d (NoSuchKey)(%s)\n", segment.ID, objectName)
						isInvalid = true
						break
					}
					fmt.Printf("get segment: %d binlog failed: %v\n", segment.ID, err)
				}
			}
		}
		if isInvalid {
			fmt.Printf("segment: %d (NoSuchKey)\n", segment.ID)
			invalidSegments++
			lostRows += segment.NumOfRows
			continue
		}
		for _, binlog := range segment.GetStatslogs() {
			for _, l := range binlog.Binlogs {
				// objectName := fmt.Sprintf("%s/stats_log/%d/%d/%d/%d/%d", p.RootPath, segment.CollectionID, segment.PartitionID, segment.ID, binlog.FieldID, l.LogPath)
				objectName := l.LogPath
				_, err = minioClient.StatObject(context.Background(), bucketName, objectName, minio.StatObjectOptions{})
				if err != nil {
					errResponse := minio.ToErrorResponse(err)
					if errResponse.Code == "NoSuchKey" {
						//fmt.Printf("segment: %d (NoSuchKey)(%s)\n", segment.ID, objectName)
						isInvalid = true
						break
					}
					fmt.Printf("get segment: %d binlog failed: %v\n", segment.ID, err)
				}
			}
		}
		if isInvalid {
			fmt.Printf("segment: %d (NoSuchKey)\n", segment.ID)
			invalidSegments++
			lostRows += segment.NumOfRows
		}
	}
	fmt.Printf("there are %d segments with %d rows is invalid\n", invalidSegments, lostRows)
	return nil
}
