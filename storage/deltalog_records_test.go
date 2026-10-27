package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/stretchr/testify/require"

	binlogv1 "github.com/milvus-io/birdwatcher/storage/binlog/v1"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

// scanTestLegacyBinlog uses the same descriptor/event layout as Milvus's
// legacy writer. Multiple payloads exercise every event, not only the first.
func scanTestLegacyBinlog(t *testing.T, dataType schemapb.DataType, eventType binlogv1.EventTypeCode, fieldID int64, payloads ...[]byte) []byte {
	t.Helper()
	var out bytes.Buffer
	write := func(value any) { require.NoError(t, binary.Write(&out, binary.LittleEndian, value)) }
	header := func(typ int8, length int32) {
		write(uint64(1))
		write(typ)
		write(length)
		write(int32(0))
	}
	write(binlogv1.MagicNumberV1)
	header(0, 0)
	write(binlogv1.DescriptorEventDataFixPart{PayloadDataType: dataType, FieldID: fieldID})
	write(make([]byte, int(binlogv1.EventTypeEnd)))
	write(int32(2))
	out.WriteString("{}")
	for _, payload := range payloads {
		header(int8(eventType), int32(17+16+len(payload)))
		write(uint64(1))
		write(uint64(1))
		out.Write(payload)
	}
	return out.Bytes()
}

func TestReadDeltalogRecordsAllFormats(t *testing.T) {
	for _, pkType := range []schemapb.DataType{schemapb.DataType_Int64, schemapb.DataType_VarChar} {
		t.Run(pkType.String(), func(t *testing.T) {
			pkArrow := arrow.PrimitiveTypes.Int64
			pk := any(int64(42))
			jsonPK := "42"
			if pkType == schemapb.DataType_VarChar {
				pkArrow = arrow.BinaryTypes.String
				pk = "key"
				jsonPK = `"key"`
			}
			var packedRows, jsonRows [][]any
			for i := range 2050 {
				packedRows = append(packedRows, []any{pk, int64(i)})
				jsonRows = append(jsonRows, []any{fmt.Sprintf(`{"pk":%s,"ts":%d,"pkType":%d}`, jsonPK, i, pkType)})
			}
			packed := scanTestParquet(t, []arrow.Field{{Name: "0", Type: pkArrow}, {Name: "1", Type: arrow.PrimitiveTypes.Int64}}, packedRows, 600)
			legacyJSON := scanTestParquet(t, []arrow.Field{{Name: "delta", Type: arrow.BinaryTypes.String}}, jsonRows, 700)
			legacyMulti := scanTestParquet(t, []arrow.Field{{Name: "pk", Type: pkArrow}, {Name: "ts", Type: arrow.PrimitiveTypes.Int64}}, packedRows, 800)
			for _, test := range []struct {
				name  string
				data  []byte
				count int64
			}{
				{"packed v2/v3", packed, 2050},
				{"legacy JSON", scanTestLegacyBinlog(t, schemapb.DataType_String, binlogv1.DeleteEventType, 0, legacyJSON, legacyJSON), 4100},
				{"legacy multifield", scanTestLegacyBinlog(t, schemapb.DataType_Int64, binlogv1.DeleteEventType, 0, legacyMulti, legacyMulti), 4100},
			} {
				t.Run(test.name, func(t *testing.T) {
					source := &scanTestSource{Reader: bytes.NewReader(test.data)}
					var calls int64
					n, err := ReadDeltalogRecords(context.Background(), source, pkType, func(value any, ts uint64) {
						require.Equal(t, pk, value)
						require.Equal(t, uint64(calls%2050), ts)
						calls++
					})
					require.NoError(t, err)
					require.Equal(t, test.count, n)
					require.Equal(t, n, calls)
					require.Equal(t, 1, source.closed)
				})
			}
		})
	}
}

func TestReadDeltalogRecordsValidatesColumnsAndClosesOnError(t *testing.T) {
	for _, test := range []struct {
		name   string
		fields []arrow.Field
		rows   [][]any
		want   string
	}{
		{"null PK", []arrow.Field{{Name: "0", Type: arrow.PrimitiveTypes.Int64, Nullable: true}, {Name: "1", Type: arrow.PrimitiveTypes.Int64}}, [][]any{{nil, int64(1)}}, "null deltalog primary key"},
		{"null timestamp", []arrow.Field{{Name: "0", Type: arrow.PrimitiveTypes.Int64}, {Name: "1", Type: arrow.PrimitiveTypes.Int64, Nullable: true}}, [][]any{{int64(1), nil}}, "invalid deltalog timestamp"},
		{"bad JSON", []arrow.Field{{Name: "delta", Type: arrow.BinaryTypes.String}}, [][]any{{`{"pk":1}`}}, "missing pkType"},
		{"wrong PK type", []arrow.Field{{Name: "0", Type: arrow.BinaryTypes.String}, {Name: "1", Type: arrow.PrimitiveTypes.Int64}}, [][]any{{"key", int64(1)}}, "primary key column type"},
	} {
		t.Run(test.name, func(t *testing.T) {
			source := &scanTestSource{Reader: bytes.NewReader(scanTestParquet(t, test.fields, test.rows, 10))}
			_, err := ReadDeltalogRecords(context.Background(), source, schemapb.DataType_Int64, func(any, uint64) { t.Fatal("invalid deletion must not be applied") })
			require.ErrorContains(t, err, test.want)
			require.Equal(t, 1, source.closed)
		})
	}
	data := scanTestParquet(t, []arrow.Field{{Name: "1", Type: arrow.PrimitiveTypes.Int64}, {Name: "0", Type: arrow.PrimitiveTypes.Int64}}, [][]any{{int64(100), int64(42)}}, 1)
	n, err := ReadDeltalogRecords(context.Background(), bytes.NewReader(data), schemapb.DataType_Int64, func(pk any, ts uint64) { require.EqualValues(t, 42, pk); require.EqualValues(t, 100, ts) })
	require.NoError(t, err)
	require.EqualValues(t, 1, n)
}
