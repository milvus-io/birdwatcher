package repair

import (
	"context"
	"fmt"
	"path"
	"strconv"
	"strings"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/birdwatcher/states/kv"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
)

type RepairL0SegmentPartitionParam struct {
	framework.ExecutionParam `use:"repair l0-segment-partition" desc:"repair L0 segment meta whose partition id is 0"`

	Collection      int64 `name:"collection" default:"0" desc:"collection id to filter with"`
	Segment         int64 `name:"segment" default:"0" desc:"segment id to filter with"`
	TargetPartition int64 `name:"targetPartition" default:"-1" desc:"target partition id to write into segment meta"`
}

// RepairL0SegmentPartitionCommand repairs dirty L0 segment metadata whose partition id is 0.
func (c *ComponentRepair) RepairL0SegmentPartitionCommand(ctx context.Context, p *RepairL0SegmentPartitionParam) error {
	if p.TargetPartition == 0 {
		return fmt.Errorf("target partition must not be 0")
	}

	segments, err := common.ListSegmentsBy(ctx, c.client, c.basePath, common.SegmentSelector{
		CollectionID: p.Collection,
		SegmentID:    p.Segment,
		Filters: []common.PostFilter[models.Segment]{
			func(segment *models.Segment) bool {
				return segment.GetPartitionID() == 0 && segment.GetLevel() == datapb.SegmentLevel_L0
			},
		},
	})
	if err != nil {
		return fmt.Errorf("failed to list L0 segments: %w", err)
	}

	repaired := 0
	for _, segment := range segments {
		fmt.Printf("repair L0 segment partition: collectionID=%d segmentID=%d oldPartitionID=0 targetPartitionID=%d\n",
			segment.GetCollectionID(), segment.GetID(), p.TargetPartition)
		if !p.Run {
			continue
		}

		if err := repairL0SegmentPartition(ctx, c.client, c.basePath, segment, p.TargetPartition); err != nil {
			return err
		}
		repaired++
	}

	if !p.Run {
		fmt.Printf("dry run L0 segment partition repair, total count: %d\n", len(segments))
	} else {
		fmt.Printf("repair L0 segment partition done, total count: %d\n", repaired)
	}
	return nil
}

func repairL0SegmentPartition(ctx context.Context, cli kv.MetaKV, basePath string, segment *models.Segment, targetPartition int64) error {
	patched := proto.Clone(segment.SegmentInfo).(*datapb.SegmentInfo)
	patched.PartitionID = targetPartition

	bs, err := proto.Marshal(patched)
	if err != nil {
		return fmt.Errorf("failed to marshal patched segment %d: %w", segment.GetID(), err)
	}

	oldKey := segment.GetKey()
	if oldKey == "" {
		oldKey = buildSegmentMetaKey(basePath, segment.GetCollectionID(), segment.GetPartitionID(), segment.GetID())
	}
	newKey := buildSegmentMetaKey(basePath, segment.GetCollectionID(), targetPartition, segment.GetID())

	if err := cli.Save(ctx, newKey, string(bs)); err != nil {
		return fmt.Errorf("failed to save patched segment %d to %s: %w", segment.GetID(), newKey, err)
	}
	if err := moveSegmentLogMeta(ctx, cli, basePath, segment.GetCollectionID(), segment.GetPartitionID(), targetPartition, segment.GetID()); err != nil {
		return err
	}
	if oldKey != newKey {
		if err := cli.Remove(ctx, oldKey); err != nil {
			return fmt.Errorf("failed to remove old segment key %s: %w", oldKey, err)
		}
	}
	return nil
}

func moveSegmentLogMeta(ctx context.Context, cli kv.MetaKV, basePath string, collectionID, oldPartitionID, targetPartitionID, segmentID int64) error {
	for _, prefix := range []string{
		"binlog",
		"deltalog",
		common.SegmentStatsMetaPrefix,
		common.SegmentBM25LogPrefix,
	} {
		oldPrefix := buildSegmentLogMetaPrefix(basePath, prefix, collectionID, oldPartitionID, segmentID)
		newPrefix := buildSegmentLogMetaPrefix(basePath, prefix, collectionID, targetPartitionID, segmentID)
		if err := movePrefix(ctx, cli, oldPrefix, newPrefix); err != nil {
			return err
		}
	}
	return nil
}

func movePrefix(ctx context.Context, cli kv.MetaKV, oldPrefix string, newPrefix string) error {
	scanPrefix := oldPrefix + "/"
	keys, values, err := cli.LoadWithPrefix(ctx, scanPrefix)
	if err != nil {
		return fmt.Errorf("failed to load prefix %s: %w", scanPrefix, err)
	}
	if len(keys) == 0 {
		return nil
	}

	newKeys := make([]string, 0, len(keys))
	for _, key := range keys {
		suffix := strings.TrimPrefix(key, oldPrefix)
		newKeys = append(newKeys, newPrefix+suffix)
	}
	if err := cli.MultiSave(ctx, newKeys, values); err != nil {
		return fmt.Errorf("failed to save moved prefix %s to %s: %w", oldPrefix, newPrefix, err)
	}
	if err := cli.RemoveWithPrefix(ctx, scanPrefix); err != nil {
		return fmt.Errorf("failed to remove old prefix %s: %w", scanPrefix, err)
	}
	return nil
}

func buildSegmentMetaKey(basePath string, collectionID, partitionID, segmentID int64) string {
	return path.Join(basePath, common.DCPrefix, common.SegmentMetaPrefix,
		strconv.FormatInt(collectionID, 10),
		strconv.FormatInt(partitionID, 10),
		strconv.FormatInt(segmentID, 10))
}

func buildSegmentLogMetaPrefix(basePath, logPrefix string, collectionID, partitionID, segmentID int64) string {
	return path.Join(basePath, common.DCPrefix, logPrefix,
		strconv.FormatInt(collectionID, 10),
		strconv.FormatInt(partitionID, 10),
		strconv.FormatInt(segmentID, 10))
}
