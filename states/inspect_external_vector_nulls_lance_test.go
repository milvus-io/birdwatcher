//go:build LANCE && cgo

package states

import (
	"errors"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

func TestShouldTryNextLanceVectorType(t *testing.T) {
	runtimeError := errors.New("read field column failed")
	require.False(t, shouldTryNextLanceVectorType(runtimeError))

	schemaError := markLanceSchemaCandidateError(errors.New("field type mismatch"))
	require.True(t, shouldTryNextLanceVectorType(schemaError))
}

func TestLanceDatasetKey(t *testing.T) {
	require.Equal(t,
		"s3://endpoint/bucket/table.lance",
		lanceDatasetKey("s3://endpoint/bucket/table.lance?fragment_id=42"),
	)
	require.Equal(t,
		"invalid-lance-path",
		lanceDatasetKey("invalid-lance-path"),
	)
}

func TestBuildLanceVectorScanIntervals(t *testing.T) {
	ranges := []externalVectorSegmentRange{
		{ObjectKey: "s3://table?fragment_id=2", StartIndex: 20, EndIndex: 25},
		{ObjectKey: "s3://table?fragment_id=1", StartIndex: 0, EndIndex: 5},
		{ObjectKey: "s3://table?fragment_id=1", StartIndex: 5, EndIndex: 10},
		{ObjectKey: "s3://table?fragment_id=2", StartIndex: 22, EndIndex: 30},
	}

	got, err := buildLanceVectorScanIntervals(ranges)
	require.NoError(t, err)
	require.Equal(t, []lanceVectorScanInterval{
		{
			ObjectKey:    "s3://table?fragment_id=1",
			StartIndex:   0,
			EndIndex:     10,
			RangeIndexes: []int{1, 2},
		},
		{
			ObjectKey:    "s3://table?fragment_id=2",
			StartIndex:   20,
			EndIndex:     30,
			RangeIndexes: []int{0, 3},
		},
	}, got)
}

func TestClassifyLanceRecordBatchAcrossIntervals(t *testing.T) {
	builder := array.NewListBuilder(memory.DefaultAllocator, arrow.PrimitiveTypes.Float32)
	defer builder.Release()
	values := builder.ValueBuilder().(*array.Float32Builder)

	builder.Append(true)
	values.AppendValues([]float32{1, 2}, nil)
	builder.Append(true)
	values.AppendNulls(2)
	builder.Append(true)
	values.Append(3)
	values.AppendNull()
	builder.Append(false)
	builder.Append(true)
	values.AppendValues([]float32{4, 5}, nil)

	input := builder.NewArray()
	defer input.Release()
	schema := arrow.NewSchema([]arrow.Field{{Name: "vector", Type: input.DataType(), Nullable: true}}, nil)
	record := array.NewRecord(schema, []arrow.Array{input}, int64(input.Len()))
	defer record.Release()

	sourceRanges := []externalVectorSegmentRange{
		{ObjectKey: "s3://table?fragment_id=1", StartIndex: 10, EndIndex: 12},
		{ObjectKey: "s3://table?fragment_id=2", StartIndex: 20, EndIndex: 23},
	}
	intervals := []lanceVectorScanInterval{
		{ObjectKey: sourceRanges[0].ObjectKey, StartIndex: 10, EndIndex: 12, RangeIndexes: []int{0}},
		{ObjectKey: sourceRanges[1].ObjectKey, StartIndex: 20, EndIndex: 23, RangeIndexes: []int{1}},
	}
	results := newExternalVectorNullResults(sourceRanges)
	intervalIndex := 0
	var intervalRowsRead int64

	err := classifyLanceRecordBatch(
		record, intervals, &intervalIndex, &intervalRowsRead,
		sourceRanges, results, schemapb.DataType_FloatVector, 2,
	)
	require.NoError(t, err)
	require.Equal(t, len(intervals), intervalIndex)
	require.Zero(t, intervalRowsRead)
	require.Equal(t, int64(1), results[0].ValidRows)
	require.Equal(t, int64(1), results[0].FullNullRows)
	require.Equal(t, int64(1), results[1].ValidRows)
	require.Equal(t, int64(1), results[1].RowNullRows)
	require.Equal(t, int64(1), results[1].PartialNullRows)
}

func TestLanceVectorCandidateTypesIncludesBinaryLayouts(t *testing.T) {
	candidates := lanceVectorCandidateTypes(schemapb.DataType_FloatVector, 8)
	var fixedSizeBinaryWidth int
	typeIDs := make(map[arrow.Type]bool)
	for _, candidate := range candidates {
		typeIDs[candidate.ID()] = true
		if fixedSizeBinary, ok := candidate.(*arrow.FixedSizeBinaryType); ok {
			fixedSizeBinaryWidth = fixedSizeBinary.ByteWidth
		}
	}

	require.Equal(t, 32, fixedSizeBinaryWidth)
	require.True(t, typeIDs[arrow.BINARY])
	require.True(t, typeIDs[arrow.LARGE_BINARY])
	require.True(t, typeIDs[arrow.BINARY_VIEW])
}
