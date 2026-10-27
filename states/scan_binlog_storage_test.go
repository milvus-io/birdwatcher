package states

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/apache/arrow/go/v17/parquet"
	"github.com/apache/arrow/go/v17/parquet/pqarrow"
	"github.com/hamba/avro/v2/ocf"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/birdwatcher/oss"
	"github.com/milvus-io/birdwatcher/states/kv"
	storagecommon "github.com/milvus-io/birdwatcher/storage/common"
	storagemanifest "github.com/milvus-io/birdwatcher/storage/manifest"
	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
)

const scanFixtureManifestSchema = `{"type":"record","name":"Manifest","fields":[
 {"name":"column_groups","type":{"type":"array","items":{"type":"record","name":"ColumnGroup","fields":[
  {"name":"columns","type":{"type":"array","items":"string"}},
  {"name":"files","type":{"type":"array","items":{"type":"record","name":"ColumnGroupFile","fields":[
   {"name":"path","type":"string"},{"name":"start_index","type":"long"},{"name":"end_index","type":"long"},
   {"name":"properties","type":{"type":"map","values":"string"}}]}}},
  {"name":"format","type":"string"}]}}},
 {"name":"delta_logs","type":{"type":"array","items":{"type":"record","name":"DeltaLog","fields":[
  {"name":"path","type":"string"},{"name":"type","type":"int"},{"name":"num_entries","type":"long"}]}}},
 {"name":"stats","type":{"type":"map","values":{"type":"record","name":"Statistics","fields":[
  {"name":"paths","type":{"type":"array","items":"string"}},{"name":"metadata","type":{"type":"map","values":"string"}}]}}},
 {"name":"indexes","type":{"type":"array","items":{"type":"record","name":"Index","fields":[
  {"name":"column_name","type":"string"},{"name":"index_type","type":"string"},{"name":"path","type":"string"},
  {"name":"properties","type":{"type":"map","values":"string"}}]}}}]}`

func scanFixtureManifest(t *testing.T, m *storagemanifest.Manifest) []byte {
	t.Helper()
	var out bytes.Buffer
	e, err := ocf.NewEncoder(scanFixtureManifestSchema, &out, ocf.WithCodec(ocf.Snappy))
	require.NoError(t, err)
	require.NoError(t, e.Encode(m))
	require.NoError(t, e.Close())
	return out.Bytes()
}

func scanFixtureParquet(t *testing.T, names []string, rows [][]int64) []byte {
	t.Helper()
	var fields []arrow.Field
	for _, name := range names {
		fields = append(fields, arrow.Field{Name: name, Type: arrow.PrimitiveTypes.Int64})
	}
	schema := arrow.NewSchema(fields, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	for _, row := range rows {
		for i, v := range row {
			b.Field(i).(*array.Int64Builder).Append(v)
		}
	}
	rec := b.NewRecord()
	defer rec.Release()
	table := array.NewTableFromRecords(schema, []arrow.Record{rec})
	defer table.Release()
	var out bytes.Buffer
	require.NoError(t, pqarrow.WriteTable(table, &out, 2, parquet.NewWriterProperties(), pqarrow.DefaultWriterProps()))
	return out.Bytes()
}

type scanFixtureKV struct {
	kv.MetaKV
	data map[string]string
}

func (s *scanFixtureKV) Load(_ context.Context, key string, _ ...kv.LoadOption) (string, error) {
	if value, ok := s.data[key]; ok {
		return value, nil
	}
	return "", kv.ErrKeyNotFound
}

func (s *scanFixtureKV) LoadWithPrefix(_ context.Context, prefix string, _ ...kv.LoadOption) ([]string, []string, error) {
	if strings.Contains(prefix, "datacoord-meta/binlog/") || strings.Contains(prefix, "datacoord-meta/deltalog/") {
		return nil, nil, fmt.Errorf("v3 scan must not depend on legacy binlog KVs: %s", prefix)
	}
	var keys, values []string
	for key := range s.data {
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	for _, key := range keys {
		values = append(values, s.data[key])
	}
	return keys, values, nil
}

type scanFixtureStore struct {
	oss.ObjectStore
	mu     sync.Mutex
	data   map[string][]byte
	opens  map[string]int
	closes map[string]int
}

type scanFixtureSource struct {
	*bytes.Reader
	close func()
}

func (s *scanFixtureSource) Close() error { s.close(); return nil }

func (s *scanFixtureStore) Open(ctx context.Context, key string, _ ...oss.OpenOption) (storagecommon.ReadSeeker, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	data, ok := s.data[key]
	if !ok {
		return nil, fmt.Errorf("missing fixture object %s", key)
	}
	s.opens[key]++
	return &scanFixtureSource{Reader: bytes.NewReader(data), close: func() { s.mu.Lock(); defer s.mu.Unlock(); s.closes[key]++ }}, nil
}

type scanFixtureStoreProvider struct{ store oss.ObjectStore }

func (p scanFixtureStoreProvider) GetObjectStore(context.Context, InstanceContext, ...oss.MinioConnectParam) (*oss.ResolvedObjectStore, error) {
	return &oss.ResolvedObjectStore{Store: p.store, RootPath: "files"}, nil
}

func newScanCommandFixture(t *testing.T) (*InstanceState, *scanFixtureStore, *datapb.SegmentInfo) {
	t.Helper()
	client := &scanFixtureKV{data: make(map[string]string)}
	put := func(key string, value proto.Message) {
		data, err := proto.Marshal(value)
		require.NoError(t, err)
		client.data[key] = string(data)
	}
	put("root/root-coord/collection/1", &etcdpb.CollectionInfo{ID: 1, Schema: &schemapb.CollectionSchema{Name: "test"}})
	put("root/root-coord/fields/1/100", &schemapb.FieldSchema{FieldID: 100, Name: "id", IsPrimaryKey: true, DataType: schemapb.DataType_Int64})
	put("root/root-coord/fields/1/101", &schemapb.FieldSchema{FieldID: 101, Name: "value", DataType: schemapb.DataType_Int64})
	segment := &datapb.SegmentInfo{
		ID: 20, CollectionID: 1, PartitionID: 2, InsertChannel: "ch1", NumOfRows: 4, State: commonpb.SegmentState_Flushed, Level: datapb.SegmentLevel_L1, StorageVersion: 3,
		ManifestPath: `{"base_path":"ROOT_PATH/insert_log/1/2/20","ver":7}`,
	}
	put("root/datacoord-meta/s/1/2/20", segment)
	base := "files/insert_log/1/2/20"
	m := &storagemanifest.Manifest{ColumnGroups: []storagemanifest.ColumnGroup{
		{Columns: []string{"100", "1"}, Format: "parquet", Files: []storagemanifest.ColumnGroupFile{{Path: "pk", EndIndex: 4}}},
		{Columns: []string{"101"}, Format: "parquet", Files: []storagemanifest.ColumnGroupFile{{Path: "value", StartIndex: 2, EndIndex: 6}}},
	}, DeltaLogs: []storagemanifest.DeltaLog{{Path: "delete", NumEntries: 3}, {NumEntries: 0}}}
	store := &scanFixtureStore{opens: make(map[string]int), closes: make(map[string]int), data: map[string][]byte{
		path.Join(base, "_metadata/manifest-7.avro"): scanFixtureManifest(t, m),
		path.Join(base, "_data/pk"):                  scanFixtureParquet(t, []string{"100", "1"}, [][]int64{{1, 10}, {1, 30}, {2, 10}, {3, 10}}),
		path.Join(base, "_data/value"):               scanFixtureParquet(t, []string{"101"}, [][]int64{{-2}, {-1}, {4}, {5}, {6}, {7}}),
		path.Join(base, "_delta/delete"):             scanFixtureParquet(t, []string{"0", "1"}, [][]int64{{1, 20}, {1, 5}, {2, 11}}),
	}}
	return &InstanceState{client: client, basePath: "root", objectStoreProvider: scanFixtureStoreProvider{store}}, store, segment
}

func captureScanOutput(t *testing.T, fn func() error) (string, error) {
	t.Helper()
	r, w, err := os.Pipe()
	require.NoError(t, err)
	old := os.Stdout
	os.Stdout = w
	defer func() { os.Stdout = old; r.Close(); w.Close() }()
	done := make(chan string, 1)
	go func() { data, _ := io.ReadAll(r); done <- string(data) }()
	err = fn()
	w.Close()
	return <-done, err
}

func TestScanBinlogStorageV3Command(t *testing.T) {
	for _, test := range []struct {
		name, action, expr, want string
		ignore                   bool
		limit                    int64
	}{
		{"count", "count", "", "Total 2 entries found", false, 0},
		{"ignore deletes", "count", "", "Total 4 entries found", true, 0},
		{"expression", "count", "value >= 6", "Total 1 entries found", false, 0},
		{"dedup", "dedup", "", "0 duplicated entries found", false, 0},
		{"dedup without deletes", "dedup", "", "1 duplicated entries found", true, 0},
		{"locate", "locate", "", "segment offset 1, pk file ROOT_PATH/insert_log/1/2/20/_data/pk, file offset 1", false, 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			s, store, _ := newScanCommandFixture(t)
			output, err := captureScanOutput(t, func() error {
				_, err := s.ScanBinlogCommand(context.Background(), &ScanBinlogParams{CollectionID: 1, SegmentID: 20, Fields: []string{"value"}, Action: test.action, Expr: test.expr, IgnoreDelete: test.ignore, OutputLimit: test.limit, WorkerNum: 2})
				return err
			})
			require.NoError(t, err)
			require.Contains(t, output, test.want)
			require.Equal(t, store.opens, store.closes)
			require.Equal(t, 1, store.opens["files/insert_log/1/2/20/_metadata/manifest-7.avro"])
			if test.ignore {
				require.Zero(t, store.opens["files/insert_log/1/2/20/_delta/delete"])
			}
		})
	}
}

func TestScanBinlogStorageV3L0WithSegmentFilter(t *testing.T) {
	s, store, _ := newScanCommandFixture(t)
	client := s.client.(*scanFixtureKV)
	for i, channel := range []string{"ch1", "ch2"} {
		l0 := &datapb.SegmentInfo{
			ID: int64(90 + i), CollectionID: 1, PartitionID: -1, InsertChannel: channel, State: commonpb.SegmentState_Flushed, Level: datapb.SegmentLevel_L0, StorageVersion: 3,
			ManifestPath: fmt.Sprintf(`{"base_path":"files/l0/%d","ver":1}`, i),
		}
		data, err := proto.Marshal(l0)
		require.NoError(t, err)
		client.data[fmt.Sprintf("root/datacoord-meta/s/1/-1/%d", l0.GetID())] = string(data)
	}
	store.data["files/l0/0/_metadata/manifest-1.avro"] = scanFixtureManifest(t, &storagemanifest.Manifest{DeltaLogs: []storagemanifest.DeltaLog{{Path: "delete", NumEntries: 1}}})
	store.data["files/l0/0/_delta/delete"] = scanFixtureParquet(t, []string{"0", "1"}, [][]int64{{3, 20}})
	output, err := captureScanOutput(t, func() error {
		_, err := s.ScanBinlogCommand(context.Background(), &ScanBinlogParams{CollectionID: 1, PartitionID: 2, SegmentID: 20, Action: "count", WorkerNum: 2})
		return err
	})
	require.NoError(t, err)
	require.Contains(t, output, "Total 1 entries found")
	require.Equal(t, store.opens, store.closes)
}

func TestScanBinlogStorageV3PropagatesLastWorkerError(t *testing.T) {
	s, store, _ := newScanCommandFixture(t)
	delete(store.data, "files/insert_log/1/2/20/_delta/delete")
	_, err := s.ScanBinlogCommand(context.Background(), &ScanBinlogParams{CollectionID: 1, SegmentID: 20, Action: "count", WorkerNum: 4})
	require.ErrorContains(t, err, "open deltalog delete")
	require.Equal(t, store.opens, store.closes)
	for _, workers := range []int64{0, -1} {
		_, err := s.ScanBinlogCommand(context.Background(), &ScanBinlogParams{WorkerNum: workers})
		require.ErrorContains(t, err, "--workerNum")
	}
}

func TestScanBinlogStorageV3RejectsMissingManifestAndAcceptsEmptySnapshot(t *testing.T) {
	for _, test := range []struct {
		name, ref string
		rows      int64
		wantError string
	}{
		{"missing", "", 4, "parse manifest reference"},
		{"invalid", `{"base_path":"files/empty"}`, 4, "nonnegative ver"},
		{"missing object", `{"base_path":"files/empty","ver":1}`, 4, "open segment 20 manifest"},
		{"empty snapshot", `{"base_path":"files/empty","ver":0}`, 0, ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			s, _, segment := newScanCommandFixture(t)
			segment.ManifestPath, segment.NumOfRows = test.ref, test.rows
			data, err := proto.Marshal(segment)
			require.NoError(t, err)
			s.client.(*scanFixtureKV).data["root/datacoord-meta/s/1/2/20"] = string(data)
			output, err := captureScanOutput(t, func() error {
				_, err := s.ScanBinlogCommand(context.Background(), &ScanBinlogParams{CollectionID: 1, Action: "count", WorkerNum: 1, Fields: []string{"value"}})
				return err
			})
			if test.wantError != "" {
				require.ErrorContains(t, err, test.wantError)
			} else {
				require.NoError(t, err)
				require.Contains(t, output, "Total 0 entries found")
			}
		})
	}
}

func TestScanBinlogStorageV3ConcurrentSegments(t *testing.T) {
	s, store, segment := newScanCommandFixture(t)
	segment.ID = 21
	data, err := proto.Marshal(segment)
	require.NoError(t, err)
	s.client.(*scanFixtureKV).data["root/datacoord-meta/s/1/2/21"] = string(data)
	output, err := captureScanOutput(t, func() error {
		_, err := s.ScanBinlogCommand(context.Background(), &ScanBinlogParams{CollectionID: 1, Action: "dedup", WorkerNum: 2})
		return err
	})
	require.NoError(t, err)
	require.Contains(t, output, "2 duplicated entries found")
	require.Equal(t, store.opens, store.closes)
	require.Equal(t, 2, store.opens["files/insert_log/1/2/20/_metadata/manifest-7.avro"])
}

func TestScanBinlogStorageV3RejectsInvalidDeltaMetadata(t *testing.T) {
	for _, test := range []struct {
		name  string
		delta storagemanifest.DeltaLog
		want  string
	}{
		{"row count", storagemanifest.DeltaLog{Path: "delete", NumEntries: 4}, "manifest declares 4"},
		{"missing path", storagemanifest.DeltaLog{NumEntries: 1}, "invalid manifest deltalog"},
		{"delete type", storagemanifest.DeltaLog{Path: "delete", NumEntries: 3, Type: storagemanifest.DeltaLogTypePositional}, "unsupported manifest deltalog type"},
	} {
		t.Run(test.name, func(t *testing.T) {
			s, store, _ := newScanCommandFixture(t)
			key := "files/insert_log/1/2/20/_metadata/manifest-7.avro"
			m, err := storagemanifest.Parse(bytes.NewReader(store.data[key]))
			require.NoError(t, err)
			m.DeltaLogs = []storagemanifest.DeltaLog{test.delta}
			store.data[key] = scanFixtureManifest(t, m)
			_, err = s.ScanBinlogCommand(context.Background(), &ScanBinlogParams{CollectionID: 1, Action: "count", WorkerNum: 1})
			require.ErrorContains(t, err, test.want)
			require.Equal(t, store.opens, store.closes)
		})
	}
}
