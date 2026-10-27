package states

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/apache/arrow/go/v17/parquet"
	"github.com/apache/arrow/go/v17/parquet/pqarrow"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/birdwatcher/configs"
	"github.com/milvus-io/birdwatcher/framework"
	storagemanifest "github.com/milvus-io/birdwatcher/storage/manifest"
	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

func TestScanBinlogFieldAndFieldsFlagsAreIndependent(t *testing.T) {
	for _, flags := range [][]string{{"--field", "value", "--fields", "id,value"}, {"--fields", "id,value", "--field", "value"}} {
		host := &struct{ *framework.CmdState }{framework.NewCmdState("test", &configs.Config{})}
		receiver := &scanBinlogFlagReceiver{}
		root := &cobra.Command{SilenceUsage: true, SilenceErrors: true}
		host.MergeFunctionCommandsFrom(root, host, receiver)
		root.SetArgs(append([]string{"scan-binlog", "--collection", "123", "--action", "count-distinct"}, flags...))
		require.NoError(t, root.Execute())
		require.Equal(t, "value", receiver.param.Field)
		require.Equal(t, []string{"id", "value"}, receiver.param.Fields)
		require.Equal(t, "count-distinct", receiver.param.Action)
	}
}

func distinctFixtureParquet(t *testing.T, dataType arrow.DataType, values []any) []byte {
	t.Helper()
	schema := arrow.NewSchema([]arrow.Field{{Name: "101", Type: dataType, Nullable: true}}, nil)
	b := array.NewBuilder(memory.DefaultAllocator, dataType)
	defer b.Release()
	for _, value := range values {
		if value == nil {
			b.AppendNull()
			continue
		}
		switch col := b.(type) {
		case *array.Int64Builder:
			col.Append(value.(int64))
		case *array.StringBuilder:
			col.Append(value.(string))
		case *array.Float64Builder:
			col.Append(value.(float64))
		default:
			t.Fatalf("unsupported test builder %T", b)
		}
	}
	col := b.NewArray()
	defer col.Release()
	rec := array.NewRecord(schema, []arrow.Array{col}, int64(len(values)))
	defer rec.Release()
	table := array.NewTableFromRecords(schema, []arrow.Record{rec})
	defer table.Release()
	var out bytes.Buffer
	require.NoError(t, pqarrow.WriteTable(table, &out, 2, parquet.NewWriterProperties(), pqarrow.DefaultWriterProps()))
	return out.Bytes()
}

func configureDistinctFixture(t *testing.T, s *InstanceState, store *scanFixtureStore, dataType schemapb.DataType, arrowType arrow.DataType, values []any) {
	t.Helper()
	field := &schemapb.FieldSchema{FieldID: 101, Name: "value", DataType: dataType, Nullable: true}
	data, err := proto.Marshal(field)
	require.NoError(t, err)
	s.client.(*scanFixtureKV).data["root/root-coord/fields/1/101"] = string(data)
	store.data["files/insert_log/1/2/20/_data/value"] = distinctFixtureParquet(t, arrowType, append([]any{nil, nil}, values...))
}

func TestScanBinlogCountDistinctFiltersAndAutoProjection(t *testing.T) {
	for _, test := range []struct {
		name, expr, field    string
		ignore               bool
		want, matched, nulls int
	}{
		{"default", "", "value", false, 1, 2, 1},
		{"ignore deletes", "", "value", true, 2, 4, 1},
		{"expression", "id == 2", "value", true, 1, 1, 0},
		{"deleted expression row", "id == 2", "value", false, 0, 0, 0},
		{"null expression row", "id == 3", "value", false, 0, 1, 1},
		{"PK field", "", "id", true, 3, 4, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			s, store, _ := newScanCommandFixture(t)
			configureDistinctFixture(t, s, store, schemapb.DataType_Int64, arrow.PrimitiveTypes.Int64, []any{int64(7), int64(7), int64(8), nil})
			p := &ScanBinlogParams{CollectionID: 1, SegmentID: 20, Action: "count-distinct", Field: test.field, Expr: test.expr, WorkerNum: 2, IgnoreDelete: test.ignore, OutputLimit: 1}
			output, err := captureScanOutput(t, func() error { _, err := s.ScanBinlogCommand(context.Background(), p); return err })
			require.NoError(t, err)
			require.Contains(t, output, fmt.Sprintf("Total %d distinct non-null values found for field %q", test.want, test.field))
			require.Contains(t, output, fmt.Sprintf("Matched rows: %d, null rows skipped: %d", test.matched, test.nulls))
			require.Empty(t, p.Fields, "auto projection must not mutate --fields")
			require.Equal(t, store.opens, store.closes)
			if test.field == "value" {
				require.Equal(t, 1, store.opens["files/insert_log/1/2/20/_data/value"])
			}
		})
	}
}

func TestScanBinlogCountDistinctSharesValuesAcrossSegments(t *testing.T) {
	s, store, segment := newScanCommandFixture(t)
	configureDistinctFixture(t, s, store, schemapb.DataType_VarChar, arrow.BinaryTypes.String, []any{"deleted", "shared", "deleted", "other"})
	segment.ID = 21
	data, err := proto.Marshal(segment)
	require.NoError(t, err)
	s.client.(*scanFixtureKV).data["root/datacoord-meta/s/1/2/21"] = string(data)
	var output string
	debug, err := captureScanDebugOutput(t, func() error {
		var err error
		output, err = captureScanOutput(t, func() error {
			_, err := s.ScanBinlogCommand(context.Background(), &ScanBinlogParams{CollectionID: 1, Action: "count-distinct", Field: "value", Fields: []string{"id", "value"}, WorkerNum: 2, Verbose: true, OutputLimit: 1})
			return err
		})
		return err
	})
	require.NoError(t, err)
	require.Contains(t, output, `Total 2 distinct non-null values found for field "value"`)
	require.Contains(t, output, "Matched rows: 4, null rows skipped: 0")
	require.Contains(t, debug, "fields.selected ids=[1 100 101]")
	require.Contains(t, debug, `distinct.target field_id=101 field_name="value" type=VarChar`)
	require.Equal(t, 2, store.opens["files/insert_log/1/2/20/_data/value"])
	require.Equal(t, store.opens, store.closes)
}

func TestScanBinlogCountDistinctUsesL0Deletes(t *testing.T) {
	s, store, _ := newScanCommandFixture(t)
	configureDistinctFixture(t, s, store, schemapb.DataType_Int64, arrow.PrimitiveTypes.Int64, []any{int64(7), int64(7), int64(7), int64(8)})
	l0 := &datapb.SegmentInfo{ID: 90, CollectionID: 1, PartitionID: -1, InsertChannel: "ch1", State: commonpb.SegmentState_Flushed, Level: datapb.SegmentLevel_L0, StorageVersion: 3, ManifestPath: `{"base_path":"files/l0/90","ver":1}`}
	data, err := proto.Marshal(l0)
	require.NoError(t, err)
	s.client.(*scanFixtureKV).data["root/datacoord-meta/s/1/-1/90"] = string(data)
	store.data["files/l0/90/_metadata/manifest-1.avro"] = scanFixtureManifest(t, &storagemanifest.Manifest{DeltaLogs: []storagemanifest.DeltaLog{{Path: "delete", NumEntries: 1}}})
	store.data["files/l0/90/_delta/delete"] = scanFixtureParquet(t, []string{"0", "1"}, [][]int64{{3, 20}})
	output, err := captureScanOutput(t, func() error {
		_, err := s.ScanBinlogCommand(context.Background(), &ScanBinlogParams{CollectionID: 1, SegmentID: 20, Action: "count-distinct", Field: "value", WorkerNum: 2})
		return err
	})
	require.NoError(t, err)
	require.Contains(t, output, "Total 1 distinct non-null values")
	require.Contains(t, output, "Matched rows: 1, null rows skipped: 0")
}

func TestScanBinlogCountDistinctValidatesArgumentsAndType(t *testing.T) {
	for _, p := range []*ScanBinlogParams{
		{Action: "count-distinct", WorkerNum: 1},
		{Action: "count-distinct", Field: " ", Fields: []string{"value"}, WorkerNum: 1},
	} {
		_, err := (&InstanceState{}).ScanBinlogCommand(context.Background(), p)
		require.ErrorContains(t, err, "requires --field")
	}
	_, err := (&InstanceState{}).ScanBinlogCommand(context.Background(), &ScanBinlogParams{Action: "count", Field: "value", WorkerNum: 1})
	require.ErrorContains(t, err, "--field is only valid")
	for _, test := range []struct {
		name, field string
		dataType    schemapb.DataType
		want        string
	}{
		{"unknown field", "missing", schemapb.DataType_Int64, "not found in collection"},
		{"JSON", "value", schemapb.DataType_JSON, "does not support field"},
		{"vector", "value", schemapb.DataType_FloatVector, "does not support field"},
	} {
		t.Run(test.name, func(t *testing.T) {
			s, store, _ := newScanCommandFixture(t)
			data, err := proto.Marshal(&schemapb.FieldSchema{FieldID: 101, Name: "value", DataType: test.dataType})
			require.NoError(t, err)
			s.client.(*scanFixtureKV).data["root/root-coord/fields/1/101"] = string(data)
			_, err = s.ScanBinlogCommand(context.Background(), &ScanBinlogParams{CollectionID: 1, Action: "count-distinct", Field: test.field, WorkerNum: 1})
			require.ErrorContains(t, err, test.want)
			require.Empty(t, store.opens)
		})
	}
}

func TestScanBinlogCountDistinctFillsAbsentDefaultAndNull(t *testing.T) {
	for _, useDefault := range []bool{false, true} {
		t.Run(fmt.Sprint(useDefault), func(t *testing.T) {
			s, store, _ := newScanCommandFixture(t)
			field := &schemapb.FieldSchema{FieldID: 101, Name: "value", DataType: schemapb.DataType_Int64, Nullable: true}
			want, nulls := 0, 2
			if useDefault {
				field.DefaultValue = &schemapb.ValueField{Data: &schemapb.ValueField_LongData{LongData: 9}}
				want, nulls = 1, 0
			}
			data, err := proto.Marshal(field)
			require.NoError(t, err)
			s.client.(*scanFixtureKV).data["root/root-coord/fields/1/101"] = string(data)
			key := "files/insert_log/1/2/20/_metadata/manifest-7.avro"
			m, err := storagemanifest.Parse(bytes.NewReader(store.data[key]))
			require.NoError(t, err)
			m.ColumnGroups = m.ColumnGroups[:1]
			store.data[key] = scanFixtureManifest(t, m)
			output, err := captureScanOutput(t, func() error {
				_, err := s.ScanBinlogCommand(context.Background(), &ScanBinlogParams{CollectionID: 1, Action: "count-distinct", Field: "value", WorkerNum: 1})
				return err
			})
			require.NoError(t, err)
			require.Contains(t, output, fmt.Sprintf("Total %d distinct non-null values", want))
			require.Contains(t, output, fmt.Sprintf("Matched rows: 2, null rows skipped: %d", nulls))
			require.Zero(t, store.opens["files/insert_log/1/2/20/_data/value"])
		})
	}
}
