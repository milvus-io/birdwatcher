package healthz

import (
	"context"
	"fmt"
	"path"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/birdwatcher/states/etcd/common"
	metakv "github.com/milvus-io/birdwatcher/states/kv"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/etcdpb"
)

type healthzCheckKV struct {
	metakv.MetaKV
	data map[string]string
}

func (k *healthzCheckKV) LoadWithPrefix(_ context.Context, prefix string, _ ...metakv.LoadOption) ([]string, []string, error) {
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

func TestStorageV1BinlogSchemaMismatchRegistered(t *testing.T) {
	item, ok := GetHealthzCheckItem("STORAGE_V1_BINLOG_SCHEMA_MISMATCH")
	require.True(t, ok)
	require.Equal(t, "STORAGE_V1_BINLOG_SCHEMA_MISMATCH", item.Name())
	require.NotEmpty(t, item.Description())
}

func TestStorageV1BinlogSchemaMismatchCheck(t *testing.T) {
	const (
		basePath     = "root"
		collectionID = int64(100)
		partitionID  = int64(10)
	)

	cli := &healthzCheckKV{data: map[string]string{
		path.Join(basePath, common.CollectionMetaPrefix, "100"): healthzProtoValue(t, &etcdpb.CollectionInfo{
			ID:     collectionID,
			Schema: &schemapb.CollectionSchema{Name: "test"},
		}),
		path.Join(basePath, common.FieldMetaPrefix, "100", "100"): healthzProtoValue(t, &schemapb.FieldSchema{FieldID: 100, Name: "pk"}),
		path.Join(basePath, common.FieldMetaPrefix, "100", "101"): healthzProtoValue(t, &schemapb.FieldSchema{FieldID: 101, Name: "value"}),
		path.Join(basePath, common.FieldMetaPrefix, "100", "102"): healthzProtoValue(t, &schemapb.FieldSchema{FieldID: 102, Name: "vector"}),

		segmentKey(basePath, collectionID, partitionID, 1):     healthzProtoValue(t, &datapb.SegmentInfo{ID: 1, CollectionID: collectionID, PartitionID: partitionID, StorageVersion: 0}),
		binlogKey(basePath, collectionID, partitionID, 1, 0):   healthzProtoValue(t, &datapb.FieldBinlog{FieldID: 0}),
		binlogKey(basePath, collectionID, partitionID, 1, 1):   healthzProtoValue(t, &datapb.FieldBinlog{FieldID: 1}),
		binlogKey(basePath, collectionID, partitionID, 1, 100): healthzProtoValue(t, &datapb.FieldBinlog{FieldID: 100}),
		binlogKey(basePath, collectionID, partitionID, 1, 102): healthzProtoValue(t, &datapb.FieldBinlog{FieldID: 102}),
		binlogKey(basePath, collectionID, partitionID, 1, 999): healthzProtoValue(t, &datapb.FieldBinlog{FieldID: 999}),

		segmentKey(basePath, collectionID, partitionID, 2):     healthzProtoValue(t, &datapb.SegmentInfo{ID: 2, CollectionID: collectionID, PartitionID: partitionID, StorageVersion: 1}),
		binlogKey(basePath, collectionID, partitionID, 2, 100): healthzProtoValue(t, &datapb.FieldBinlog{FieldID: 100}),
		binlogKey(basePath, collectionID, partitionID, 2, 101): healthzProtoValue(t, &datapb.FieldBinlog{FieldID: 101}),
		binlogKey(basePath, collectionID, partitionID, 2, 999): healthzProtoValue(t, &datapb.FieldBinlog{FieldID: 999}),

		segmentKey(basePath, collectionID, partitionID, 3):     healthzProtoValue(t, &datapb.SegmentInfo{ID: 3, CollectionID: collectionID, PartitionID: partitionID, StorageVersion: 2}),
		binlogKey(basePath, collectionID, partitionID, 3, 100): healthzProtoValue(t, &datapb.FieldBinlog{FieldID: 100}),
	}}

	item := newStorageV1BinlogSchemaMismatch()
	reports, err := item.Check(context.Background(), cli, basePath)
	require.NoError(t, err)
	require.Len(t, reports, 2)

	require.Equal(t, item.Name(), reports[0].Item)
	require.EqualValues(t, 1, reports[0].Extra["segment_id"])
	require.EqualValues(t, collectionID, reports[0].Extra["collection_id"])
	require.EqualValues(t, 0, reports[0].Extra["storage_version"])
	require.Equal(t, []int64{101}, reports[0].Extra["missing_field_ids"])
	require.Equal(t, []int64{100, 101, 102}, reports[0].Extra["schema_field_ids"])
	require.Equal(t, []int64{0, 1, 100, 102, 999}, reports[0].Extra["binlog_field_ids"])

	require.Equal(t, item.Name(), reports[1].Item)
	require.EqualValues(t, 2, reports[1].Extra["segment_id"])
	require.EqualValues(t, collectionID, reports[1].Extra["collection_id"])
	require.EqualValues(t, 1, reports[1].Extra["storage_version"])
	require.Equal(t, []int64{102}, reports[1].Extra["missing_field_ids"])
	require.Equal(t, []int64{100, 101, 999}, reports[1].Extra["binlog_field_ids"])
}

func segmentKey(basePath string, collectionID, partitionID, segmentID int64) string {
	return path.Join(basePath, common.DCPrefix, common.SegmentMetaPrefix, formatID(collectionID), formatID(partitionID), formatID(segmentID))
}

func binlogKey(basePath string, collectionID, partitionID, segmentID, fieldID int64) string {
	return path.Join(basePath, common.DCPrefix, "binlog", formatID(collectionID), formatID(partitionID), formatID(segmentID), formatID(fieldID))
}

func formatID(id int64) string {
	return fmt.Sprintf("%d", id)
}

func healthzProtoValue(t *testing.T, value proto.Message) string {
	t.Helper()
	bs, err := proto.Marshal(value)
	require.NoError(t, err)
	return string(bs)
}
