package repair

import (
	"context"
	"path"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/birdwatcher/states/kv"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
)

type l0SegmentRepairKV struct {
	kv.MetaKV
	data map[string]string
}

func (k *l0SegmentRepairKV) Load(ctx context.Context, key string, opts ...kv.LoadOption) (string, error) {
	value, ok := k.data[key]
	if !ok {
		return "", kv.ErrKeyNotFound
	}
	return value, nil
}

func (k *l0SegmentRepairKV) LoadWithPrefix(ctx context.Context, prefix string, opts ...kv.LoadOption) ([]string, []string, error) {
	keys := make([]string, 0)
	for key := range k.data {
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)

	values := make([]string, 0, len(keys))
	for _, key := range keys {
		values = append(values, k.data[key])
	}
	return keys, values, nil
}

func (k *l0SegmentRepairKV) Save(ctx context.Context, key, value string) error {
	k.data[key] = value
	return nil
}

func (k *l0SegmentRepairKV) MultiSave(ctx context.Context, keys, values []string) error {
	for i, key := range keys {
		k.data[key] = values[i]
	}
	return nil
}

func (k *l0SegmentRepairKV) Remove(ctx context.Context, key string) error {
	delete(k.data, key)
	return nil
}

func (k *l0SegmentRepairKV) RemoveWithPrefix(ctx context.Context, prefix string) error {
	for key := range k.data {
		if strings.HasPrefix(key, prefix) {
			delete(k.data, key)
		}
	}
	return nil
}

func (k *l0SegmentRepairKV) WalkWithPrefix(ctx context.Context, prefix string, paginationSize int, fn func([]byte, []byte) error) error {
	keys, values, err := k.LoadWithPrefix(ctx, prefix)
	if err != nil {
		return err
	}
	for i, key := range keys {
		if err := fn([]byte(key), []byte(values[i])); err != nil {
			return err
		}
	}
	return nil
}

func TestRepairL0SegmentPartitionDryRunDoesNotMoveMeta(t *testing.T) {
	ctx := context.Background()
	basePath := "test-root/meta"
	cli := newL0SegmentRepairKV(t, basePath,
		&datapb.SegmentInfo{
			ID:           10,
			CollectionID: 100,
			PartitionID:  0,
			Level:        datapb.SegmentLevel_L0,
		},
	)
	oldKey := segmentMetaKey(basePath, 100, 0, 10)

	comp := &ComponentRepair{client: cli, basePath: basePath}
	err := comp.RepairL0SegmentPartitionCommand(ctx, &RepairL0SegmentPartitionParam{
		TargetPartition: -1,
	})
	require.NoError(t, err)

	require.Contains(t, cli.data, oldKey)
	require.NotContains(t, cli.data, segmentMetaKey(basePath, 100, -1, 10))
}

func TestRepairL0SegmentPartitionMovesSegmentAndLogMeta(t *testing.T) {
	ctx := context.Background()
	basePath := "test-root/meta"
	cli := newL0SegmentRepairKV(t, basePath,
		&datapb.SegmentInfo{
			ID:           10,
			CollectionID: 100,
			PartitionID:  0,
			Level:        datapb.SegmentLevel_L0,
		},
		&datapb.SegmentInfo{
			ID:           11,
			CollectionID: 100,
			PartitionID:  0,
			Level:        datapb.SegmentLevel_L1,
		},
	)
	oldDeltaKey := path.Join(basePath, common.DCPrefix, common.SegmentStatsMetaPrefix, "100", "0", "10", "101")
	cli.data[oldDeltaKey] = "statslog-value"
	oldSegmentKey := segmentMetaKey(basePath, 100, 0, 10)
	newSegmentKey := segmentMetaKey(basePath, 100, -1, 10)
	newDeltaKey := path.Join(basePath, common.DCPrefix, common.SegmentStatsMetaPrefix, "100", "-1", "10", "101")

	comp := &ComponentRepair{client: cli, basePath: basePath}
	err := comp.RepairL0SegmentPartitionCommand(ctx, &RepairL0SegmentPartitionParam{
		ExecutionParam:  framework.ExecutionParam{Run: true},
		TargetPartition: -1,
	})
	require.NoError(t, err)

	require.NotContains(t, cli.data, oldSegmentKey)
	require.Contains(t, cli.data, newSegmentKey)
	require.NotContains(t, cli.data, oldDeltaKey)
	require.Equal(t, "statslog-value", cli.data[newDeltaKey])

	patched := &datapb.SegmentInfo{}
	require.NoError(t, proto.Unmarshal([]byte(cli.data[newSegmentKey]), patched))
	require.EqualValues(t, -1, patched.GetPartitionID())

	require.Contains(t, cli.data, segmentMetaKey(basePath, 100, 0, 11))
	require.NotContains(t, cli.data, segmentMetaKey(basePath, 100, -1, 11))
}

func TestRepairL0SegmentPartitionAllowsCustomTargetPartition(t *testing.T) {
	ctx := context.Background()
	basePath := "test-root/meta"
	cli := newL0SegmentRepairKV(t, basePath,
		&datapb.SegmentInfo{
			ID:           10,
			CollectionID: 100,
			PartitionID:  0,
			Level:        datapb.SegmentLevel_L0,
		},
	)

	comp := &ComponentRepair{client: cli, basePath: basePath}
	err := comp.RepairL0SegmentPartitionCommand(ctx, &RepairL0SegmentPartitionParam{
		ExecutionParam:  framework.ExecutionParam{Run: true},
		TargetPartition: 99,
	})
	require.NoError(t, err)

	require.NotContains(t, cli.data, segmentMetaKey(basePath, 100, 0, 10))
	require.Contains(t, cli.data, segmentMetaKey(basePath, 100, 99, 10))
}

func newL0SegmentRepairKV(t *testing.T, basePath string, segments ...*datapb.SegmentInfo) *l0SegmentRepairKV {
	t.Helper()

	cli := &l0SegmentRepairKV{data: make(map[string]string)}
	for _, segment := range segments {
		bs, err := proto.Marshal(segment)
		require.NoError(t, err)
		cli.data[segmentMetaKey(basePath, segment.GetCollectionID(), segment.GetPartitionID(), segment.GetID())] = string(bs)
	}
	return cli
}

func segmentMetaKey(basePath string, collectionID, partitionID, segmentID int64) string {
	return path.Join(basePath, common.DCPrefix, common.SegmentMetaPrefix,
		formatInt64(collectionID), formatInt64(partitionID), formatInt64(segmentID))
}

func formatInt64(v int64) string {
	return strconv.FormatInt(v, 10)
}
