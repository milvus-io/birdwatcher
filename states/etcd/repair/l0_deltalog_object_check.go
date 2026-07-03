package repair

import (
	"context"
	"fmt"
	"path"
	"strconv"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/oss"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/birdwatcher/states/kv"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
)

type CheckL0DeltalogObjectParam struct {
	framework.ParamBase `use:"repair check-l0-deltalog-object" desc:"stream check L0 segment deltalog object consistency"`

	Collection      int64  `name:"collection" default:"0" desc:"collection id to filter with"`
	Segment         int64  `name:"segment" default:"0" desc:"segment id to filter with"`
	MinioAddress    string `name:"minioAddr" default:"" desc:"override minio address, leave empty to use milvus.yaml value"`
	SkipBucketCheck bool   `name:"skipBucketCheck" default:"false" desc:"skip bucket existence check when connecting object storage"`
}

type l0DeltalogCheckSummary struct {
	Segments          int
	SegmentsWithDelta int
	Objects           int
	Exists            int
	Missing           int
	ZeroLogID         int
	NoDeltalog        int
}

// CheckL0DeltalogObjectCommand checks whether every L0 segment deltalog meta has an existing object.
func (c *ComponentRepair) CheckL0DeltalogObjectCommand(ctx context.Context, p *CheckL0DeltalogObjectParam) error {
	params := []oss.MinioConnectParam{oss.WithSkipCheckBucket(p.SkipBucketCheck)}
	if p.MinioAddress != "" {
		params = append(params, oss.WithMinioAddr(p.MinioAddress))
	}
	resolvedStore, err := c.getObjectStore(ctx, params...)
	if err != nil {
		return err
	}
	minioClient, ok := oss.MinioClientFromObjectStore(resolvedStore.Store)
	if !ok {
		return fmt.Errorf("resolved object store is not backed by minio client")
	}
	checker := &minioDeltalogObjectCopier{
		client: minioClient,
		bucket: resolvedStore.BucketName,
	}

	summary, err := checkL0DeltalogObjects(ctx, c.client, checker, c.basePath, resolvedStore.RootPath, p.Collection, p.Segment)
	if err != nil {
		return err
	}
	fmt.Printf("check L0 deltalog object done, segments: %d, segmentsWithDelta: %d, objects: %d, exists: %d, missing: %d, zeroLogID: %d, noDeltalog: %d\n",
		summary.Segments, summary.SegmentsWithDelta, summary.Objects, summary.Exists, summary.Missing, summary.ZeroLogID, summary.NoDeltalog)
	if summary.Missing > 0 || summary.ZeroLogID > 0 || summary.NoDeltalog > 0 {
		return fmt.Errorf("found inconsistent L0 deltalogs: missing=%d zeroLogID=%d noDeltalog=%d",
			summary.Missing, summary.ZeroLogID, summary.NoDeltalog)
	}
	return nil
}

func checkL0DeltalogObjects(
	ctx context.Context,
	cli kv.MetaKV,
	checker deltalogObjectCopier,
	basePath string,
	rootPath string,
	collectionID int64,
	segmentID int64,
) (*l0DeltalogCheckSummary, error) {
	summary := &l0DeltalogCheckSummary{}
	prefix := path.Join(basePath, common.DCPrefix, common.SegmentMetaPrefix) + "/"
	if collectionID > 0 {
		prefix = path.Join(basePath, common.DCPrefix, common.SegmentMetaPrefix, strconv.FormatInt(collectionID, 10)) + "/"
	}

	err := common.WalkWithPrefix(ctx, cli, prefix, 1000, func(key []byte, value []byte) error {
		segment, err := unmarshalSegmentInfo(key, value)
		if err != nil {
			return err
		}
		if !shouldCheckL0Segment(segment, collectionID, segmentID) {
			return nil
		}

		summary.Segments++
		fields, err := loadL0DeltalogMetas(ctx, cli, basePath, segment)
		if err != nil {
			return fmt.Errorf("failed to load L0 deltalog meta: collectionID=%d partitionID=%d segmentID=%d: %w",
				segment.GetCollectionID(), segment.GetPartitionID(), segment.GetID(), err)
		}
		if len(fields) == 0 {
			fmt.Printf("NO_DELTALOG collectionID=%d partitionID=%d segmentID=%d\n",
				segment.GetCollectionID(), segment.GetPartitionID(), segment.GetID())
			summary.NoDeltalog++
			return nil
		}
		summary.SegmentsWithDelta++

		for _, field := range fields {
			for _, binlog := range field.GetBinlogs() {
				if binlog.GetLogID() == 0 {
					fmt.Printf("ZERO_LOG_ID collectionID=%d partitionID=%d segmentID=%d fieldID=%d\n",
						segment.GetCollectionID(), segment.GetPartitionID(), segment.GetID(), field.GetFieldID())
					summary.ZeroLogID++
					continue
				}

				objectKey := buildDeltalogObjectKey(rootPath, segment.GetCollectionID(), segment.GetPartitionID(), segment.GetID(), binlog.GetLogID())
				summary.Objects++
				exists, err := checker.ObjectExists(ctx, objectKey)
				if err != nil {
					return fmt.Errorf("failed to stat L0 deltalog object %s: %w", objectKey, err)
				}
				if !exists {
					fmt.Printf("MISSING collectionID=%d partitionID=%d segmentID=%d fieldID=%d logID=%d key=%s\n",
						segment.GetCollectionID(), segment.GetPartitionID(), segment.GetID(), field.GetFieldID(), binlog.GetLogID(), objectKey)
					summary.Missing++
					continue
				}
				fmt.Printf("OK collectionID=%d partitionID=%d segmentID=%d fieldID=%d logID=%d key=%s\n",
					segment.GetCollectionID(), segment.GetPartitionID(), segment.GetID(), field.GetFieldID(), binlog.GetLogID(), objectKey)
				summary.Exists++
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return summary, nil
}

func loadL0DeltalogMetas(ctx context.Context, cli kv.MetaKV, basePath string, segment *datapb.SegmentInfo) ([]*datapb.FieldBinlog, error) {
	prefix := path.Join(basePath, common.DCPrefix, "deltalog",
		strconv.FormatInt(segment.GetCollectionID(), 10),
		strconv.FormatInt(segment.GetPartitionID(), 10),
		strconv.FormatInt(segment.GetID(), 10)) + "/"
	fields, _, err := common.ListProtoObjects[datapb.FieldBinlog](ctx, cli, prefix)
	if err != nil {
		return nil, err
	}
	return fields, nil
}

func shouldCheckL0Segment(segment *datapb.SegmentInfo, collectionID int64, segmentID int64) bool {
	return segment.GetLevel() == datapb.SegmentLevel_L0 &&
		(collectionID == 0 || segment.GetCollectionID() == collectionID) &&
		(segmentID == 0 || segment.GetID() == segmentID)
}

func unmarshalSegmentInfo(key []byte, value []byte) (*datapb.SegmentInfo, error) {
	segment := &datapb.SegmentInfo{}
	if err := proto.Unmarshal(value, segment); err != nil {
		return nil, fmt.Errorf("failed to unmarshal segment meta %s: %w", string(key), err)
	}
	return segment, nil
}
