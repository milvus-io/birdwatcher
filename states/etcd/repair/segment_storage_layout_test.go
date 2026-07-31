package repair

import (
	"context"
	"path"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/birdwatcher/states/kv"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
)

type segmentStorageLayoutKV struct {
	kv.MetaKV
	data           map[string]string
	multiSaveKeys  [][]string
	multiSaveValue [][]string
}

func (s *segmentStorageLayoutKV) Load(ctx context.Context, key string, opts ...kv.LoadOption) (string, error) {
	value, ok := s.data[key]
	if !ok {
		return "", kv.ErrKeyNotFound
	}
	return value, nil
}

func (s *segmentStorageLayoutKV) LoadWithPrefix(ctx context.Context, prefix string, opts ...kv.LoadOption) ([]string, []string, error) {
	keys := make([]string, 0)
	for key := range s.data {
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)

	values := make([]string, 0, len(keys))
	for _, key := range keys {
		values = append(values, s.data[key])
	}
	return keys, values, nil
}

func (s *segmentStorageLayoutKV) MultiSave(ctx context.Context, keys, values []string) error {
	s.multiSaveKeys = append(s.multiSaveKeys, append([]string(nil), keys...))
	s.multiSaveValue = append(s.multiSaveValue, append([]string(nil), values...))
	for idx, key := range keys {
		s.data[key] = values[idx]
	}
	return nil
}

func TestParseColumnGroupMappings(t *testing.T) {
	mappings, err := parseColumnGroupMappings([]string{"0:100:1:0", "101:101"})
	require.NoError(t, err)
	require.Equal(t, map[int64][]int64{
		0:   {0, 1, 100},
		101: {101},
	}, mappings)
}

func TestParseColumnGroupMappingsRejectsInvalidInput(t *testing.T) {
	tests := []struct {
		name  string
		input []string
	}{
		{name: "empty", input: nil},
		{name: "missing children", input: []string{"100"}},
		{name: "duplicate group", input: []string{"100:100", "100:101"}},
		{name: "duplicate child in group", input: []string{"100:100:100"}},
		{name: "negative id", input: []string{"-1:100"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := parseColumnGroupMappings(test.input)
			require.Error(t, err)
		})
	}
}

func TestPrepareSegmentStorageLayout(t *testing.T) {
	segment := &datapb.SegmentInfo{
		ID:             10,
		StorageVersion: 1,
		DataVersion:    7,
	}
	records := []fieldBinlogRecord{
		{
			key: "binlog/0",
			value: &datapb.FieldBinlog{
				FieldID: 0,
			},
		},
		{
			key: "binlog/101",
			value: &datapb.FieldBinlog{
				FieldID:     101,
				ChildFields: []int64{999},
			},
		},
	}
	mappings := map[int64][]int64{
		0:   {0, 1, 100},
		101: {101},
	}
	validFields := map[int64]struct{}{
		0: {}, 1: {}, 100: {}, 101: {},
	}

	change, err := prepareSegmentStorageLayout(segment, records, storageVersionV2, mappings, validFields, false)
	require.NoError(t, err)
	require.True(t, change.changed)
	require.EqualValues(t, storageVersionV2, change.segment.GetStorageVersion())
	require.EqualValues(t, 8, change.segment.GetDataVersion())
	require.Equal(t, []int64{0, 1, 100}, change.fieldBinlogs[0].GetChildFields())
	require.Equal(t, []int64{101}, change.fieldBinlogs[1].GetChildFields())
	require.Equal(t, []string{"binlog/0", "binlog/101"}, change.fieldBinlogKeys)

	require.EqualValues(t, 1, segment.GetStorageVersion(), "input segment must not be mutated")
	require.Empty(t, records[0].value.GetChildFields(), "input FieldBinlog must not be mutated")
	require.Equal(t, []int64{999}, records[1].value.GetChildFields(), "input FieldBinlog must not be mutated")
}

func TestPrepareSegmentStorageLayoutNoChange(t *testing.T) {
	segment := &datapb.SegmentInfo{
		ID:             10,
		StorageVersion: storageVersionV2,
		DataVersion:    7,
	}
	records := []fieldBinlogRecord{{
		key: "binlog/100",
		value: &datapb.FieldBinlog{
			FieldID:     100,
			ChildFields: []int64{101, 100},
		},
	}}

	change, err := prepareSegmentStorageLayout(
		segment,
		records,
		storageVersionV2,
		map[int64][]int64{100: {100, 101}},
		map[int64]struct{}{100: {}, 101: {}},
		false,
	)
	require.NoError(t, err)
	require.False(t, change.changed)
	require.EqualValues(t, 7, change.segment.GetDataVersion())
	require.Equal(t, []int64{100, 101}, change.fieldBinlogs[0].GetChildFields())
}

func TestPrepareSegmentStorageLayoutRequiresCompleteMapping(t *testing.T) {
	records := []fieldBinlogRecord{
		{key: "binlog/100", value: &datapb.FieldBinlog{FieldID: 100}},
		{key: "binlog/101", value: &datapb.FieldBinlog{FieldID: 101}},
	}

	_, err := prepareSegmentStorageLayout(
		&datapb.SegmentInfo{},
		records,
		storageVersionV2,
		map[int64][]int64{100: {100}, 102: {102}},
		map[int64]struct{}{100: {}, 102: {}},
		false,
	)
	require.ErrorContains(t, err, "missing=[101]")
	require.ErrorContains(t, err, "unknown=[102]")
}

func TestPrepareSegmentStorageLayoutRejectsDuplicateAndUnknownChildren(t *testing.T) {
	records := []fieldBinlogRecord{
		{key: "binlog/100", value: &datapb.FieldBinlog{FieldID: 100}},
		{key: "binlog/101", value: &datapb.FieldBinlog{FieldID: 101}},
	}

	_, err := prepareSegmentStorageLayout(
		&datapb.SegmentInfo{},
		records,
		storageVersionV2,
		map[int64][]int64{100: {100}, 101: {100}},
		map[int64]struct{}{100: {}},
		false,
	)
	require.ErrorContains(t, err, "belongs to both column group")

	_, err = prepareSegmentStorageLayout(
		&datapb.SegmentInfo{},
		records,
		storageVersionV2,
		map[int64][]int64{100: {100}, 101: {999}},
		map[int64]struct{}{100: {}},
		false,
	)
	require.ErrorContains(t, err, "not present in the current collection schema")

	_, err = prepareSegmentStorageLayout(
		&datapb.SegmentInfo{},
		records,
		storageVersionV2,
		map[int64][]int64{100: {100}, 101: {999}},
		nil,
		true,
	)
	require.NoError(t, err)
}

func TestRepairSegmentStorageLayoutCommandBacksUpAndUpdatesAtomically(t *testing.T) {
	const (
		basePath    = "root"
		collection  = int64(100)
		partition   = int64(20)
		segmentID   = int64(10)
		dataVersion = int32(7)
	)
	segmentKey := path.Join(basePath, common.DCPrefix, common.SegmentMetaPrefix, "100", "20", "10")
	field0Key := path.Join(basePath, common.DCPrefix, "binlog", "100", "20", "10", "0")
	field101Key := path.Join(basePath, common.DCPrefix, "binlog", "100", "20", "10", "101")

	segment := &datapb.SegmentInfo{
		ID:             segmentID,
		CollectionID:   collection,
		PartitionID:    partition,
		State:          commonpb.SegmentState_Flushed,
		StorageVersion: 1,
		DataVersion:    dataVersion,
	}
	field0 := &datapb.FieldBinlog{FieldID: 0}
	field101 := &datapb.FieldBinlog{FieldID: 101, ChildFields: []int64{999}}

	mustMarshal := func(message proto.Message) string {
		value, err := proto.Marshal(message)
		require.NoError(t, err)
		return string(value)
	}
	originalSegment := mustMarshal(segment)
	originalField0 := mustMarshal(field0)
	originalField101 := mustMarshal(field101)
	cli := &segmentStorageLayoutKV{data: map[string]string{
		segmentKey:  originalSegment,
		field0Key:   originalField0,
		field101Key: originalField101,
	}}
	component := NewComponent(cli, nil, basePath)

	err := component.RepairSegmentStorageLayoutCommand(context.Background(), &RepairSegmentStorageLayoutParam{
		ExecutionParam:     framework.ExecutionParam{Run: true},
		SegmentID:          segmentID,
		StorageVersion:     storageVersionV2,
		ColumnGroups:       []string{"0:0:1:100", "101:101"},
		AllowUnknownFields: true,
	})
	require.NoError(t, err)
	require.Len(t, cli.multiSaveKeys, 2)
	require.Len(t, cli.multiSaveValue, 2)

	backupPrefix := path.Join("birdwatcher/backup/segment-storage-layout") + "/"
	require.Len(t, cli.multiSaveKeys[0], 3)
	for _, key := range cli.multiSaveKeys[0] {
		require.True(t, strings.HasPrefix(key, backupPrefix), key)
	}
	require.ElementsMatch(t, []string{originalSegment, originalField0, originalField101}, cli.multiSaveValue[0])

	require.Equal(t, []string{segmentKey, field0Key, field101Key}, cli.multiSaveKeys[1])
	updatedSegment := &datapb.SegmentInfo{}
	require.NoError(t, proto.Unmarshal([]byte(cli.data[segmentKey]), updatedSegment))
	require.EqualValues(t, storageVersionV2, updatedSegment.GetStorageVersion())
	require.EqualValues(t, dataVersion+1, updatedSegment.GetDataVersion())

	updatedField0 := &datapb.FieldBinlog{}
	require.NoError(t, proto.Unmarshal([]byte(cli.data[field0Key]), updatedField0))
	require.Equal(t, []int64{0, 1, 100}, updatedField0.GetChildFields())
	updatedField101 := &datapb.FieldBinlog{}
	require.NoError(t, proto.Unmarshal([]byte(cli.data[field101Key]), updatedField101))
	require.Equal(t, []int64{101}, updatedField101.GetChildFields())
}
