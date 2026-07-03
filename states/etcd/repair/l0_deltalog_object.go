package repair

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"

	"github.com/minio/minio-go/v7"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/oss"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
)

type RepairL0DeltalogObjectParam struct {
	framework.ExecutionParam `use:"repair l0-deltalog-object" desc:"copy L0 deltalog objects from source partition to target partition"`

	Collection      int64  `name:"collection" default:"0" desc:"collection id to filter with"`
	SegmentFile     string `name:"segmentFile" default:"" desc:"file containing segment ids to repair"`
	SourcePartition int64  `name:"sourcePartition" default:"0" desc:"source partition id in deltalog object path"`
	TargetPartition int64  `name:"targetPartition" default:"-1" desc:"target partition id in deltalog object path"`
	MinioAddress    string `name:"minioAddr" default:"" desc:"override minio address, leave empty to use milvus.yaml value"`
	SkipBucketCheck bool   `name:"skipBucketCheck" default:"false" desc:"skip bucket existence check when connecting object storage"`
}

// RepairL0DeltalogObjectCommand copies L0 deltalog objects using paths derived from deltalog log IDs.
func (c *ComponentRepair) RepairL0DeltalogObjectCommand(ctx context.Context, p *RepairL0DeltalogObjectParam) error {
	if p.SegmentFile == "" {
		return fmt.Errorf("segmentFile is required")
	}
	if p.SourcePartition == p.TargetPartition {
		return fmt.Errorf("source partition and target partition must be different")
	}

	segmentIDs, err := readSegmentIDFile(p.SegmentFile)
	if err != nil {
		return err
	}
	if len(segmentIDs) == 0 {
		return fmt.Errorf("no segment ids found in %s", p.SegmentFile)
	}

	segments, err := common.ListSegmentsBy(ctx, c.client, c.basePath, common.SegmentSelector{
		CollectionID: p.Collection,
		Filters: []common.PostFilter[models.Segment]{
			func(segment *models.Segment) bool {
				if segment.GetLevel() != datapb.SegmentLevel_L0 {
					return false
				}
				_, ok := segmentIDs[segment.GetID()]
				return ok
			},
		},
	})
	if err != nil {
		return fmt.Errorf("failed to list L0 segments from file %s: %w", p.SegmentFile, err)
	}
	found := make(map[int64]struct{}, len(segments))
	for _, segment := range segments {
		found[segment.GetID()] = struct{}{}
	}
	if missing := missingIDs(segmentIDs, found); len(missing) > 0 {
		return fmt.Errorf("L0 segment ids not found: %s", formatIDs(missing))
	}

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
	copier := &minioDeltalogObjectCopier{
		client: minioClient,
		bucket: resolvedStore.BucketName,
	}

	copied, skipped, missingObjects, err := copyL0DeltalogObjects(ctx, copier, resolvedStore.RootPath, segments, p.SourcePartition, p.TargetPartition, p.Run)
	if err != nil {
		return err
	}
	if !p.Run {
		fmt.Printf("dry run L0 deltalog object copy, copy candidates: %d, skipped: %d, source missing: %d\n", copied, skipped, missingObjects)
	} else {
		fmt.Printf("copy L0 deltalog object done, copied: %d, skipped: %d, source missing: %d\n", copied, skipped, missingObjects)
	}
	return nil
}

type deltalogObjectCopier interface {
	ObjectExists(ctx context.Context, key string) (bool, error)
	CopyObject(ctx context.Context, sourceKey, targetKey string) error
}

type minioDeltalogObjectCopier struct {
	client *minio.Client
	bucket string
}

func (c *minioDeltalogObjectCopier) ObjectExists(ctx context.Context, key string) (bool, error) {
	_, err := c.client.StatObject(ctx, c.bucket, key, minio.StatObjectOptions{})
	if err == nil {
		return true, nil
	}
	resp := minio.ToErrorResponse(err)
	switch resp.Code {
	case "NoSuchKey", "NotFound", "NoSuchVersion":
		return false, nil
	default:
		return false, err
	}
}

func (c *minioDeltalogObjectCopier) CopyObject(ctx context.Context, sourceKey, targetKey string) error {
	_, err := c.client.CopyObject(ctx, minio.CopyDestOptions{
		Bucket: c.bucket,
		Object: targetKey,
	}, minio.CopySrcOptions{
		Bucket: c.bucket,
		Object: sourceKey,
	})
	return err
}

func copyL0DeltalogObjects(
	ctx context.Context,
	copier deltalogObjectCopier,
	rootPath string,
	segments []*models.Segment,
	sourcePartition int64,
	targetPartition int64,
	run bool,
) (copied int, skipped int, missingObjects int, err error) {
	for _, segment := range segments {
		for _, fieldBinlog := range segment.GetDeltalogs() {
			for _, binlog := range fieldBinlog.Binlogs {
				if binlog.LogID == 0 {
					skipped++
					continue
				}

				sourceKey := buildDeltalogObjectKey(rootPath, segment.GetCollectionID(), sourcePartition, segment.GetID(), binlog.LogID)
				targetKey := buildDeltalogObjectKey(rootPath, segment.GetCollectionID(), targetPartition, segment.GetID(), binlog.LogID)
				exists, err := copier.ObjectExists(ctx, sourceKey)
				if err != nil {
					return copied, skipped, missingObjects, fmt.Errorf("failed to stat source deltalog object %s: %w", sourceKey, err)
				}
				if !exists {
					fmt.Printf("source deltalog object missing: segmentID=%d logID=%d key=%s\n", segment.GetID(), binlog.LogID, sourceKey)
					missingObjects++
					continue
				}
				exists, err = copier.ObjectExists(ctx, targetKey)
				if err != nil {
					return copied, skipped, missingObjects, fmt.Errorf("failed to stat target deltalog object %s: %w", targetKey, err)
				}
				if exists {
					fmt.Printf("target deltalog object exists, skip copy: segmentID=%d logID=%d key=%s\n", segment.GetID(), binlog.LogID, targetKey)
					skipped++
					continue
				}

				fmt.Printf("copy L0 deltalog object: segmentID=%d logID=%d source=%s target=%s\n", segment.GetID(), binlog.LogID, sourceKey, targetKey)
				copied++
				if !run {
					continue
				}
				if err := copier.CopyObject(ctx, sourceKey, targetKey); err != nil {
					return copied, skipped, missingObjects, fmt.Errorf("failed to copy deltalog object %s to %s: %w", sourceKey, targetKey, err)
				}
			}
		}
	}
	return copied, skipped, missingObjects, nil
}

func buildDeltalogObjectKey(rootPath string, collectionID, partitionID, segmentID, logID int64) string {
	return oss.ResolveObjectKey(rootPath, path.Join("ROOT_PATH", "delta_log",
		strconv.FormatInt(collectionID, 10),
		strconv.FormatInt(partitionID, 10),
		strconv.FormatInt(segmentID, 10),
		strconv.FormatInt(logID, 10)))
}

func readSegmentIDFile(filePath string) (map[int64]struct{}, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open segment file %s: %w", filePath, err)
	}
	defer file.Close()

	result := make(map[int64]struct{})
	scanner := bufio.NewScanner(file)
	lineNo := 0
	for scanner.Scan() {
		lineNo++
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		segmentID, err := strconv.ParseInt(line, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid segment id at %s:%d: %s", filePath, lineNo, line)
		}
		result[segmentID] = struct{}{}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("failed to read segment file %s: %w", filePath, err)
	}
	return result, nil
}

func missingIDs(want map[int64]struct{}, got map[int64]struct{}) []int64 {
	missing := make([]int64, 0)
	for id := range want {
		if _, ok := got[id]; !ok {
			missing = append(missing, id)
		}
	}
	sort.Slice(missing, func(i, j int) bool { return missing[i] < missing[j] })
	return missing
}

func formatIDs(ids []int64) string {
	values := make([]string, 0, len(ids))
	for _, id := range ids {
		values = append(values, strconv.FormatInt(id, 10))
	}
	return strings.Join(values, ",")
}
