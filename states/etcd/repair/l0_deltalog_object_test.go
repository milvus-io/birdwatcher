package repair

import (
	"context"
	"path"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
)

type recordingDeltalogObjectCopier struct {
	existing map[string]struct{}
	copies   [][2]string
}

func (c *recordingDeltalogObjectCopier) ObjectExists(ctx context.Context, key string) (bool, error) {
	if _, ok := c.existing[key]; !ok {
		return false, nil
	}
	return true, nil
}

func (c *recordingDeltalogObjectCopier) CopyObject(ctx context.Context, sourceKey, targetKey string) error {
	c.copies = append(c.copies, [2]string{sourceKey, targetKey})
	return nil
}

func TestCopyL0DeltalogObjectsBuildsPathsFromLogIDs(t *testing.T) {
	ctx := context.Background()
	segment := newL0DeltalogObjectSegment(100, -1, 10, []*datapb.FieldBinlog{{
		FieldID: 101,
		Binlogs: []*datapb.Binlog{
			{LogID: 1001},
			{LogID: 1002},
		},
	}})
	copier := &recordingDeltalogObjectCopier{
		existing: map[string]struct{}{
			"bucket-root/delta_log/100/0/10/1001": {},
		},
	}

	copied, skipped, missingObjects, err := copyL0DeltalogObjects(ctx, copier, "bucket-root", []*models.Segment{segment}, 0, -1, true)
	require.NoError(t, err)
	require.Equal(t, 1, copied)
	require.Equal(t, 0, skipped)
	require.Equal(t, 1, missingObjects)
	require.Equal(t, [][2]string{{
		"bucket-root/delta_log/100/0/10/1001",
		"bucket-root/delta_log/100/-1/10/1001",
	}}, copier.copies)
}

func TestCopyL0DeltalogObjectsDryRunDoesNotCopy(t *testing.T) {
	ctx := context.Background()
	segment := newL0DeltalogObjectSegment(100, -1, 10, []*datapb.FieldBinlog{{
		FieldID: 101,
		Binlogs: []*datapb.Binlog{{LogID: 1001}},
	}})
	copier := &recordingDeltalogObjectCopier{
		existing: map[string]struct{}{
			"bucket-root/delta_log/100/0/10/1001": {},
		},
	}

	copied, skipped, missingObjects, err := copyL0DeltalogObjects(ctx, copier, "bucket-root", []*models.Segment{segment}, 0, -1, false)
	require.NoError(t, err)
	require.Equal(t, 1, copied)
	require.Equal(t, 0, skipped)
	require.Equal(t, 0, missingObjects)
	require.Empty(t, copier.copies)
}

func TestCopyL0DeltalogObjectsSkipsExistingTarget(t *testing.T) {
	ctx := context.Background()
	segment := newL0DeltalogObjectSegment(100, -1, 10, []*datapb.FieldBinlog{{
		FieldID: 101,
		Binlogs: []*datapb.Binlog{{LogID: 1001}},
	}})
	copier := &recordingDeltalogObjectCopier{
		existing: map[string]struct{}{
			"bucket-root/delta_log/100/0/10/1001":  {},
			"bucket-root/delta_log/100/-1/10/1001": {},
		},
	}

	copied, skipped, missingObjects, err := copyL0DeltalogObjects(ctx, copier, "bucket-root", []*models.Segment{segment}, 0, -1, true)
	require.NoError(t, err)
	require.Equal(t, 0, copied)
	require.Equal(t, 1, skipped)
	require.Equal(t, 0, missingObjects)
	require.Empty(t, copier.copies)
}

func TestCopyL0DeltalogObjectsSkipsZeroLogID(t *testing.T) {
	ctx := context.Background()
	segment := newL0DeltalogObjectSegment(100, -1, 10, []*datapb.FieldBinlog{{
		FieldID: 101,
		Binlogs: []*datapb.Binlog{{LogID: 0}},
	}})
	copier := &recordingDeltalogObjectCopier{existing: map[string]struct{}{}}

	copied, skipped, missingObjects, err := copyL0DeltalogObjects(ctx, copier, "bucket-root", []*models.Segment{segment}, 0, -1, true)
	require.NoError(t, err)
	require.Equal(t, 0, copied)
	require.Equal(t, 1, skipped)
	require.Equal(t, 0, missingObjects)
	require.Empty(t, copier.copies)
}

func TestBuildDeltalogObjectKey(t *testing.T) {
	require.Equal(t, "bucket-root/delta_log/100/-1/10/1001", buildDeltalogObjectKey("bucket-root", 100, -1, 10, 1001))
	require.Equal(t, "delta_log/100/0/10/1001", buildDeltalogObjectKey("", 100, 0, 10, 1001))
}

func TestCheckL0DeltalogObjectsStreamsExpectedPartitionPath(t *testing.T) {
	ctx := context.Background()
	basePath := "test-root/meta"
	cli := newL0SegmentRepairKV(t, basePath,
		&datapb.SegmentInfo{
			ID:           10,
			CollectionID: 100,
			PartitionID:  -1,
			Level:        datapb.SegmentLevel_L0,
		},
		&datapb.SegmentInfo{
			ID:           11,
			CollectionID: 100,
			PartitionID:  0,
			Level:        datapb.SegmentLevel_L1,
		},
	)
	cli.data[deltalogMetaKey(basePath, 100, -1, 10, 101)] = mustFieldBinlogValue(t, &datapb.FieldBinlog{
		FieldID: 101,
		Binlogs: []*datapb.Binlog{
			{LogID: 1001},
			{LogID: 1002},
		},
	})
	checker := &recordingDeltalogObjectCopier{
		existing: map[string]struct{}{
			"bucket-root/delta_log/100/-1/10/1001": {},
		},
	}

	summary, err := checkL0DeltalogObjects(ctx, cli, checker, basePath, "bucket-root", 0, 0)
	require.NoError(t, err)
	require.Equal(t, &l0DeltalogCheckSummary{
		Segments:          1,
		SegmentsWithDelta: 1,
		Objects:           2,
		Exists:            1,
		Missing:           1,
	}, summary)
}

func TestCheckL0DeltalogObjectsReportsNoDeltalogAndZeroLogID(t *testing.T) {
	ctx := context.Background()
	basePath := "test-root/meta"
	cli := newL0SegmentRepairKV(t, basePath,
		&datapb.SegmentInfo{
			ID:           10,
			CollectionID: 100,
			PartitionID:  -1,
			Level:        datapb.SegmentLevel_L0,
		},
		&datapb.SegmentInfo{
			ID:           11,
			CollectionID: 100,
			PartitionID:  -1,
			Level:        datapb.SegmentLevel_L0,
		},
	)
	cli.data[deltalogMetaKey(basePath, 100, -1, 10, 101)] = mustFieldBinlogValue(t, &datapb.FieldBinlog{
		FieldID: 101,
		Binlogs: []*datapb.Binlog{
			{LogID: 0},
		},
	})

	summary, err := checkL0DeltalogObjects(ctx, cli, &recordingDeltalogObjectCopier{existing: map[string]struct{}{}}, basePath, "bucket-root", 100, 0)
	require.NoError(t, err)
	require.Equal(t, &l0DeltalogCheckSummary{
		Segments:          2,
		SegmentsWithDelta: 1,
		ZeroLogID:         1,
		NoDeltalog:        1,
	}, summary)
}

func newL0DeltalogObjectSegment(collectionID, partitionID, segmentID int64, deltalogs []*datapb.FieldBinlog) *models.Segment {
	return models.NewSegment(&datapb.SegmentInfo{
		ID:           segmentID,
		CollectionID: collectionID,
		PartitionID:  partitionID,
		Level:        datapb.SegmentLevel_L0,
	}, "", func() ([]*datapb.FieldBinlog, []*datapb.FieldBinlog, []*datapb.FieldBinlog, []*datapb.FieldBinlog, error) {
		return nil, nil, deltalogs, nil, nil
	})
}

func deltalogMetaKey(basePath string, collectionID, partitionID, segmentID, fieldID int64) string {
	return path.Join(basePath, common.DCPrefix, "deltalog",
		formatInt64(collectionID), formatInt64(partitionID), formatInt64(segmentID), formatInt64(fieldID))
}

func mustFieldBinlogValue(t *testing.T, info *datapb.FieldBinlog) string {
	t.Helper()

	bs, err := proto.Marshal(info)
	require.NoError(t, err)
	return string(bs)
}

var _ deltalogObjectCopier = (*recordingDeltalogObjectCopier)(nil)
