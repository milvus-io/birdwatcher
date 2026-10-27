package tasks

import (
	"math"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

func TestCountDistinctScalarValues(t *testing.T) {
	for _, test := range []struct {
		name     string
		dataType schemapb.DataType
		values   []any
		want     int64
	}{
		{"bool", schemapb.DataType_Bool, []any{false, false, true, nil}, 2},
		{"int8", schemapb.DataType_Int8, []any{int8(-128), int8(127), int8(-128), nil}, 2},
		{"int16", schemapb.DataType_Int16, []any{int16(-32768), int16(32767), int16(-32768), nil}, 2},
		{"int32", schemapb.DataType_Int32, []any{int32(math.MinInt32), int32(math.MaxInt32), int32(math.MaxInt32), nil}, 2},
		{"int64 precision", schemapb.DataType_Int64, []any{int64(1 << 53), int64(1<<53 + 1), int64(math.MaxInt64), int64(math.MinInt64), int64(math.MaxInt64), nil}, 4},
		{"string", schemapb.DataType_String, []any{"", "", "a", "A", "a\x00b", "ab", nil}, 5},
		{"varchar", schemapb.DataType_VarChar, []any{"a", "a", "b", "", nil}, 3},
		{"float32", schemapb.DataType_Float, []any{float32(0), float32(math.Copysign(0, -1)), float32(1), float32(1), math.Float32frombits(0x7fc00001), math.Float32frombits(0xffc00002), float32(math.Inf(1)), float32(math.Inf(-1)), nil}, 5},
		{"float64", schemapb.DataType_Double, []any{float64(0), math.Copysign(0, -1), float64(1), float64(1), math.Float64frombits(0x7ff8000000000001), math.Float64frombits(0xfff8000000000002), math.Inf(1), math.Inf(-1), nil}, 5},
	} {
		t.Run(test.name, func(t *testing.T) {
			task, err := NewCountDistinctTask(&schemapb.FieldSchema{FieldID: 101, Name: "target", DataType: test.dataType})
			require.NoError(t, err)
			for _, value := range test.values {
				require.NoError(t, task.Scan(nil, nil, 0, map[int64]any{101: value, 102: "ignored"}))
			}
			require.Equal(t, test.want, task.Counter())
			require.EqualValues(t, len(test.values), task.matched.Load())
			require.EqualValues(t, 1, task.nulls.Load())
		})
	}
}

func TestCountDistinctEmptyAndNullOnly(t *testing.T) {
	task, err := NewCountDistinctTask(&schemapb.FieldSchema{FieldID: 101, Name: "target", DataType: schemapb.DataType_Int64})
	require.NoError(t, err)
	require.Zero(t, task.Counter())
	require.Zero(t, task.matched.Load())
	require.Zero(t, task.nulls.Load())
	for range 3 {
		require.NoError(t, task.Scan(nil, nil, 0, map[int64]any{101: nil}))
	}
	require.Zero(t, task.Counter())
	require.EqualValues(t, 3, task.nulls.Load())
}

func TestCountDistinctRejectsUnsupportedSchemaAndInvalidRow(t *testing.T) {
	_, err := NewCountDistinctTask(nil)
	require.ErrorContains(t, err, "requires a field schema")
	for _, dataType := range []schemapb.DataType{schemapb.DataType_None, schemapb.DataType_Array, schemapb.DataType_JSON, schemapb.DataType_FloatVector, schemapb.DataType_Text} {
		_, err := NewCountDistinctTask(&schemapb.FieldSchema{FieldID: 101, Name: "target", DataType: dataType})
		require.ErrorContains(t, err, "does not support field")
	}
	task, err := NewCountDistinctTask(&schemapb.FieldSchema{FieldID: 101, Name: "target", DataType: schemapb.DataType_Int64})
	require.NoError(t, err)
	require.ErrorContains(t, task.Scan(nil, nil, 0, map[int64]any{102: int64(1)}), "missing from the scanned row")
	for _, value := range []any{float64(1), int32(1), "1", []int64{1}, map[string]int{"a": 1}} {
		require.ErrorContains(t, task.Scan(nil, nil, 0, map[int64]any{101: value}), "expects Int64")
	}
	require.Zero(t, task.Counter())
	require.Zero(t, task.matched.Load())
}

func TestCountDistinctConcurrentWorkersShareSet(t *testing.T) {
	task, err := NewCountDistinctTask(&schemapb.FieldSchema{FieldID: 101, Name: "target", DataType: schemapb.DataType_Int64})
	require.NoError(t, err)
	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 500 {
				var value any = int64(i % 100)
				if i%101 == 0 {
					value = nil
				}
				if err := task.Scan(nil, nil, 0, map[int64]any{101: value}); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.EqualValues(t, 100, task.Counter())
	require.EqualValues(t, 8000, task.matched.Load())
	require.EqualValues(t, 80, task.nulls.Load())
}
