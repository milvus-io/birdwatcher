package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/apache/arrow/go/v17/parquet"
	"github.com/apache/arrow/go/v17/parquet/pqarrow"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/storage/common"
	"github.com/milvus-io/birdwatcher/storage/manifest"
	"github.com/milvus-io/birdwatcher/storage/tasks"
	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

func scanTestParquet(t *testing.T, fields []arrow.Field, rows [][]any, rowGroupSize int64) []byte {
	t.Helper()
	schema := arrow.NewSchema(fields, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	for _, row := range rows {
		for i, value := range row {
			if value == nil {
				b.Field(i).AppendNull()
				continue
			}
			switch col := b.Field(i).(type) {
			case *array.Int64Builder:
				col.Append(value.(int64))
			case *array.Int32Builder:
				col.Append(value.(int32))
			case *array.StringBuilder:
				col.Append(value.(string))
			case *array.FixedSizeBinaryBuilder:
				col.Append(value.([]byte))
			default:
				t.Fatalf("unsupported test builder %T", col)
			}
		}
	}
	rec := b.NewRecord()
	defer rec.Release()
	table := array.NewTableFromRecords(schema, []arrow.Record{rec})
	defer table.Release()
	var out bytes.Buffer
	require.NoError(t, pqarrow.WriteTable(table, &out, rowGroupSize, parquet.NewWriterProperties(), pqarrow.DefaultWriterProps()))
	return out.Bytes()
}

type scanTestSource struct {
	*bytes.Reader
	closed int
}

func (s *scanTestSource) Close() error { s.closed++; return nil }

func scanTestFields(pkType schemapb.DataType) map[int64]*schemapb.FieldSchema {
	return map[int64]*schemapb.FieldSchema{
		1:   {FieldID: 1, Name: "Timestamp", DataType: schemapb.DataType_Int64},
		100: {FieldID: 100, Name: "id", DataType: pkType, IsPrimaryKey: true},
		101: {FieldID: 101, Name: "value", DataType: schemapb.DataType_Int64},
	}
}

func scanTestSegment() *models.Segment {
	return &models.Segment{SegmentInfo: &datapb.SegmentInfo{
		ID: 20, StorageVersion: 3,
		ManifestPath: `{"base_path":"files/insert_log/1/2/20","ver":7}`,
	}}
}

func TestManifestSegmentReaderAlignsIndependentFragments(t *testing.T) {
	// PK/timestamp files contribute 3+7 rows; value files contribute 5+5.
	// Both use nonzero windows and independently sized Parquet row groups.
	var pkRows, valueRows [][]any
	for i := range 13 {
		pkRows = append(pkRows, []any{int64(100 + i), int64(1000 + i), int64(-1)})
		valueRows = append(valueRows, []any{int64(i * 10)})
	}
	objects := map[string][]byte{
		"pk-a":   scanTestParquet(t, []arrow.Field{{Name: "100", Type: arrow.PrimitiveTypes.Int64}, {Name: "1", Type: arrow.PrimitiveTypes.Int64}, {Name: "999", Type: arrow.PrimitiveTypes.Int64}}, pkRows, 2),
		"pk-b":   scanTestParquet(t, []arrow.Field{{Name: "100", Type: arrow.PrimitiveTypes.Int64}, {Name: "1", Type: arrow.PrimitiveTypes.Int64}, {Name: "999", Type: arrow.PrimitiveTypes.Int64}}, pkRows, 4),
		"values": scanTestParquet(t, []arrow.Field{{Name: "101", Type: arrow.PrimitiveTypes.Int64}}, valueRows, 3),
	}
	m := &manifest.Manifest{ColumnGroups: []manifest.ColumnGroup{
		{Columns: []string{"100", "1", "999"}, Format: "parquet", Files: []manifest.ColumnGroupFile{{Path: "pk-a", StartIndex: 1, EndIndex: 4}, {Path: "pk-b", StartIndex: 4, EndIndex: 11}}},
		{Columns: []string{"101"}, Format: "parquet", Files: []manifest.ColumnGroupFile{{Path: "values", StartIndex: 1, EndIndex: 6}, {Path: "values", StartIndex: 6, EndIndex: 11}}},
	}}
	var opened []*scanTestSource
	r, err := NewManifestSegmentReader(scanTestSegment(), m, scanTestFields(schemapb.DataType_Int64), func(name string) (common.ReadSeeker, error) {
		data, ok := objects[name]
		require.True(t, ok)
		s := &scanTestSource{Reader: bytes.NewReader(data)}
		opened = append(opened, s)
		return s, nil
	})
	require.NoError(t, err)
	defer r.Close()
	var total int64
	for {
		batch, info, err := r.Next(context.Background())
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		require.Positive(t, batch.Len())
		require.Equal(t, total, info.SegmentOffset)
		require.Nil(t, batch.Column(999))
		for row := range batch.Len() {
			logical := total + int64(row)
			require.Equal(t, int64(101)+logical, batch.Column(100).(*array.Int64).Value(row))
			require.Equal(t, int64(1001)+logical, batch.Column(1).(*array.Int64).Value(row))
			require.Equal(t, (logical+1)*10, batch.Column(101).(*array.Int64).Value(row))
			require.Equal(t, logical+1, info.FileOffsets[100]+int64(row))
		}
		total += int64(batch.Len())
	}
	require.EqualValues(t, 10, total)
	require.NoError(t, r.Close())
	require.Len(t, opened, 4)
	for _, s := range opened {
		require.Equal(t, 1, s.closed)
	}
}

func TestManifestSegmentReaderCrossesArrowBatchesAndFillsAbsent(t *testing.T) {
	var rows [][]any
	for i := range 2051 {
		rows = append(rows, []any{fmt.Sprint(i), int64(i)})
	}
	data := scanTestParquet(t, []arrow.Field{{Name: "100", Type: arrow.BinaryTypes.String}, {Name: "1", Type: arrow.PrimitiveTypes.Int64}}, rows, 1500)
	fields := scanTestFields(schemapb.DataType_VarChar)
	fields[101].DefaultValue = &schemapb.ValueField{Data: &schemapb.ValueField_LongData{LongData: 42}}
	fields[102] = &schemapb.FieldSchema{FieldID: 102, Name: "optional", DataType: schemapb.DataType_VarChar, Nullable: true}
	m := &manifest.Manifest{ColumnGroups: []manifest.ColumnGroup{{Columns: []string{"100", "1"}, Format: "parquet", Files: []manifest.ColumnGroupFile{{Path: "data", StartIndex: 1, EndIndex: 2050}}}}}
	r, err := NewManifestSegmentReader(scanTestSegment(), m, fields, func(string) (common.ReadSeeker, error) { return bytes.NewReader(data), nil })
	require.NoError(t, err)
	defer r.Close()
	var count int
	for {
		batch, _, err := r.Next(context.Background())
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		for row := range batch.Len() {
			count++
			require.Equal(t, fmt.Sprint(count), batch.Column(100).(*array.String).Value(row))
			require.EqualValues(t, 42, batch.Column(101).(*array.Int64).Value(row))
			require.True(t, batch.Column(102).IsNull(row))
		}
	}
	require.Equal(t, 2049, count)
}

func TestManifestSegmentReaderRejectsCorruptMetadataAndFiles(t *testing.T) {
	validGroup := func() manifest.ColumnGroup {
		return manifest.ColumnGroup{Columns: []string{"100", "1", "101"}, Format: "parquet", Files: []manifest.ColumnGroupFile{{Path: "data", EndIndex: 2}}}
	}
	for _, test := range []struct {
		name   string
		change func(*manifest.Manifest)
		want   string
	}{
		{"range", func(m *manifest.Manifest) { m.ColumnGroups[0].Files[0].StartIndex = -1 }, "invalid file range"},
		{"length mismatch", func(m *manifest.Manifest) {
			g := validGroup()
			g.Files[0].EndIndex = 3
			m.ColumnGroups = append(m.ColumnGroups, g)
		}, "expected 2"},
		{"duplicate", func(m *manifest.Manifest) { m.ColumnGroups = append(m.ColumnGroups, validGroup()) }, "duplicate manifest column"},
		{"missing PK", func(m *manifest.Manifest) { m.ColumnGroups[0].Columns = []string{"1", "101"} }, "missing required field 100"},
		{"format", func(m *manifest.Manifest) { m.ColumnGroups[0].Format = "vortex" }, "unsupported storage v3 format"},
	} {
		t.Run(test.name, func(t *testing.T) {
			m := &manifest.Manifest{ColumnGroups: []manifest.ColumnGroup{validGroup()}}
			test.change(m)
			_, err := NewManifestSegmentReader(scanTestSegment(), m, scanTestFields(schemapb.DataType_Int64), nil)
			require.ErrorContains(t, err, test.want)
		})
	}
	data := scanTestParquet(t, []arrow.Field{{Name: "100", Type: arrow.PrimitiveTypes.Int64}, {Name: "1", Type: arrow.PrimitiveTypes.Int64}}, [][]any{{int64(1), int64(10)}}, 1)
	for _, test := range []struct {
		name string
		data []byte
		want string
	}{
		{"range exceeds file", data, "exceeds 1 rows"},
		{"truncated", data[:len(data)-5], "open parquet"},
	} {
		t.Run(test.name, func(t *testing.T) {
			source := &scanTestSource{Reader: bytes.NewReader(test.data)}
			r, err := NewManifestSegmentReader(scanTestSegment(), &manifest.Manifest{ColumnGroups: []manifest.ColumnGroup{validGroup()}}, scanTestFields(schemapb.DataType_Int64), func(string) (common.ReadSeeker, error) { return source, nil })
			require.NoError(t, err)
			_, _, err = r.Next(context.Background())
			require.ErrorContains(t, err, test.want)
			require.NoError(t, r.Close())
			require.Equal(t, 1, source.closed)
		})
	}
}

func TestManifestIteratorUsesDeleteAndExpressionFilters(t *testing.T) {
	fields := scanTestFields(schemapb.DataType_Int64)
	data := scanTestParquet(t, []arrow.Field{{Name: "100", Type: arrow.PrimitiveTypes.Int64}, {Name: "1", Type: arrow.PrimitiveTypes.Int64}, {Name: "101", Type: arrow.PrimitiveTypes.Int64}},
		[][]any{{int64(1), int64(10), int64(9)}, {int64(1), int64(30), int64(10)}, {int64(2), int64(10), int64(5)}, {int64(3), int64(10), int64(1)}}, 2)
	m := &manifest.Manifest{ColumnGroups: []manifest.ColumnGroup{{Columns: []string{"100", "1", "101"}, Format: "parquet", Files: []manifest.ColumnGroupFile{{Path: "data", EndIndex: 4}}}}}
	source := &scanTestSource{Reader: bytes.NewReader(data)}
	r, err := NewManifestSegmentReader(scanTestSegment(), m, fields, func(string) (common.ReadSeeker, error) { return source, nil })
	require.NoError(t, err)
	expr, err := NewExprFilter(fields, "value >= 5")
	require.NoError(t, err)
	task := tasks.NewCountTask()
	iter := NewSegmentIterator(scanTestSegment(), &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{fields[100]}}, []EntryFilter{NewDeltalogFilter(map[any]uint64{int64(1): 20}), expr}, fields, nil, task)
	require.NoError(t, iter.RangeReader(context.Background(), r))
	require.EqualValues(t, 2, task.Counter())
	require.Equal(t, 1, source.closed)
}

func TestManifestIteratorClosesOnEarlyExitAndCancellation(t *testing.T) {
	fields := scanTestFields(schemapb.DataType_Int64)
	data := scanTestParquet(t, []arrow.Field{{Name: "100", Type: arrow.PrimitiveTypes.Int64}, {Name: "1", Type: arrow.PrimitiveTypes.Int64}, {Name: "101", Type: arrow.PrimitiveTypes.Int64}}, [][]any{{int64(1), int64(10), int64(5)}, {int64(2), int64(10), int64(5)}}, 2)
	m := &manifest.Manifest{ColumnGroups: []manifest.ColumnGroup{{Columns: []string{"100", "1", "101"}, Format: "parquet", Files: []manifest.ColumnGroupFile{{Path: "data", EndIndex: 2}}}}}
	source := &scanTestSource{Reader: bytes.NewReader(data)}
	r, err := NewManifestSegmentReader(scanTestSegment(), m, fields, func(string) (common.ReadSeeker, error) { return source, nil })
	require.NoError(t, err)
	task := tasks.NewLocateTask(1, models.FieldSchema{FieldID: 100})
	iter := NewSegmentIterator(scanTestSegment(), &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{fields[100]}}, nil, fields, nil, task)
	require.ErrorIs(t, iter.RangeReader(context.Background(), r), io.EOF)
	require.Equal(t, 1, source.closed)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r, err = NewManifestSegmentReader(scanTestSegment(), m, fields, func(string) (common.ReadSeeker, error) { t.Fatal("canceled scan must not open files"); return nil, nil })
	require.NoError(t, err)
	_, _, err = r.Next(ctx)
	require.ErrorIs(t, err, context.Canceled)
	require.NoError(t, r.Close())
}

func TestManifestSegmentReaderVectorAndNullCells(t *testing.T) {
	fields := scanTestFields(schemapb.DataType_Int64)
	fields[101] = &schemapb.FieldSchema{FieldID: 101, Name: "vector", DataType: schemapb.DataType_FloatVector, Nullable: true, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "2"}}}
	vector := []byte{0, 0, 128, 63, 0, 0, 0, 64} // float32 values 1, 2
	data := scanTestParquet(t, []arrow.Field{{Name: "100", Type: arrow.PrimitiveTypes.Int64}, {Name: "1", Type: arrow.PrimitiveTypes.Int64}, {Name: "101", Type: &arrow.FixedSizeBinaryType{ByteWidth: 8}, Nullable: true}}, [][]any{{int64(1), int64(10), vector}, {int64(2), int64(10), nil}}, 2)
	m := &manifest.Manifest{ColumnGroups: []manifest.ColumnGroup{{Columns: []string{"100", "1", "101"}, Format: "parquet", Files: []manifest.ColumnGroupFile{{Path: "data", EndIndex: 2}}}}}
	r, err := NewManifestSegmentReader(scanTestSegment(), m, fields, func(string) (common.ReadSeeker, error) { return bytes.NewReader(data), nil })
	require.NoError(t, err)
	defer r.Close()
	batch, _, err := r.Next(context.Background())
	require.NoError(t, err)
	value, ok := DeserializeItem(batch.Column(101), schemapb.DataType_FloatVector, 0)
	require.True(t, ok)
	require.Equal(t, []float32{1, 2}, value)
	value, ok = DeserializeItem(batch.Column(101), schemapb.DataType_FloatVector, 1)
	require.True(t, ok)
	require.Nil(t, value)
}
