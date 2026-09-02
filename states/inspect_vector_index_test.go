package states

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
)

func TestInspectVectorIndexValidityRequiresFinishedIndex(t *testing.T) {
	field := &schemapb.FieldSchema{
		FieldID:  101,
		Name:     "embedding",
		DataType: schemapb.DataType_FloatVector,
	}
	fieldIndex := &indexpb.IndexInfo{FieldID: field.GetFieldID()}

	t.Run("in progress", func(t *testing.T) {
		segmentIndex := &indexpb.SegmentIndex{
			SegmentID: 1,
			NumRows:   1000,
			State:     commonpb.IndexState_InProgress,
			IndexType: "HNSW",
		}
		record := inspectVectorIndexValidity(
			context.Background(), nil, segmentIndex, fieldIndex, field)

		require.Equal(t, "InProgress", record.IndexState)
		require.Equal(t, "INDEX_NOT_FINISHED", record.Status)
		require.Zero(t, record.ValidRows)
		require.Zero(t, record.NullRows)
		require.Empty(t, record.InspectionError)
	})

	t.Run("failed", func(t *testing.T) {
		segmentIndex := &indexpb.SegmentIndex{
			SegmentID:  2,
			NumRows:    1000,
			State:      commonpb.IndexState_Failed,
			FailReason: "invalid vector row",
			IndexType:  "HNSW",
		}
		record := inspectVectorIndexValidity(
			context.Background(), nil, segmentIndex, fieldIndex, field)

		require.Equal(t, "Failed", record.IndexState)
		require.Equal(t, "INDEX_FAILED", record.Status)
		require.Equal(t, "invalid vector row", record.FailReason)
		require.Zero(t, record.ValidRows)
		require.Zero(t, record.NullRows)
		require.Empty(t, record.InspectionError)
	})

	t.Run("finished", func(t *testing.T) {
		segmentIndex := &indexpb.SegmentIndex{
			SegmentID: 3,
			NumRows:   1000,
			State:     commonpb.IndexState_Finished,
			IndexType: "HNSW",
		}
		record := inspectVectorIndexValidity(
			context.Background(), nil, segmentIndex, fieldIndex, field)

		require.Equal(t, "Finished", record.IndexState)
		require.Equal(t, "INFERRED_ALL_VALID", record.Status)
		require.Equal(t, int64(1000), record.ValidRows)
	})

	t.Run("finished with artifacts requires object store", func(t *testing.T) {
		segmentIndex := &indexpb.SegmentIndex{
			SegmentID:     4,
			NumRows:       1000,
			State:         commonpb.IndexState_Finished,
			IndexType:     "HNSW",
			IndexFileKeys: []string{"valid_data_count", "valid_data"},
		}
		record := inspectVectorIndexValidity(
			context.Background(), nil, segmentIndex, fieldIndex, field)

		require.Equal(t, "ERROR", record.Status)
		require.Equal(t,
			"object store is required to inspect valid_data artifacts",
			record.InspectionError,
		)
	})
}

func TestDecodeMemoryVectorValidity(t *testing.T) {
	countPayload := make([]byte, 8)
	binary.LittleEndian.PutUint64(countPayload, 1000)
	bitmap := make([]byte, 125)
	for index := 0; index < 118; index++ {
		bitmap[index] = 0xff
	}
	bitmap[118] = 0x3f

	totalRows, validRows, err := decodeMemoryVectorValidity(countPayload, bitmap)
	require.NoError(t, err)
	require.Equal(t, uint64(1000), totalRows)
	require.Equal(t, uint64(950), validRows)
}

func TestDecodeDiskVectorValidityAllNull(t *testing.T) {
	payload := make([]byte, 8+125)
	binary.LittleEndian.PutUint64(payload, 1000)

	totalRows, validRows, err := decodeDiskVectorValidity(payload)
	require.NoError(t, err)
	require.Equal(t, uint64(1000), totalRows)
	require.Zero(t, validRows)
}

func TestCountVectorValidityBitmapIgnoresTailPadding(t *testing.T) {
	validRows, err := countVectorValidityBitmap(10, []byte{0xff, 0xff})
	require.NoError(t, err)
	require.Equal(t, uint64(10), validRows)
}

func TestDecodeVectorValidityRejectsInvalidPayloads(t *testing.T) {
	_, _, err := decodeMemoryVectorValidity(make([]byte, 7), nil)
	require.ErrorContains(t, err, "must be 8 bytes")

	countPayload := make([]byte, 8)
	binary.LittleEndian.PutUint64(countPayload, 9)
	_, _, err = decodeMemoryVectorValidity(countPayload, []byte{0xff})
	require.ErrorContains(t, err, "needs 2 bytes")

	_, _, err = decodeDiskVectorValidity(make([]byte, 7))
	require.ErrorContains(t, err, "8-byte count")
}

func TestClassifyVectorValidityArtifacts(t *testing.T) {
	t.Run("memory", func(t *testing.T) {
		artifacts, err := classifyVectorValidityArtifacts([]string{
			"other-index-file",
			"valid_data",
			"valid_data_count",
		})
		require.NoError(t, err)
		require.Equal(t, vectorValidityMemory, artifacts.kind)
		require.Len(t, artifacts.countParts, 1)
		require.Len(t, artifacts.dataParts, 1)
	})

	t.Run("disk slices are sorted", func(t *testing.T) {
		artifacts, err := classifyVectorValidityArtifacts([]string{
			"valid_data_1",
			"valid_data_0",
		})
		require.NoError(t, err)
		require.Equal(t, vectorValidityDisk, artifacts.kind)
		require.Equal(t, 0, artifacts.dataParts[0].index)
		require.Equal(t, 1, artifacts.dataParts[1].index)
	})

	t.Run("missing slice", func(t *testing.T) {
		_, err := classifyVectorValidityArtifacts([]string{"valid_data_1"})
		require.ErrorContains(t, err, "expected slice 0")
	})

	t.Run("incomplete memory artifacts", func(t *testing.T) {
		_, err := classifyVectorValidityArtifacts([]string{"valid_data_count"})
		require.ErrorContains(t, err, "no valid_data artifact")
	})

	t.Run("unrelated files", func(t *testing.T) {
		artifacts, err := classifyVectorValidityArtifacts([]string{"index_data"})
		require.NoError(t, err)
		require.Equal(t, vectorValidityAbsent, artifacts.kind)
	})
}

func TestVectorIndexNeedsObjectStore(t *testing.T) {
	tests := []struct {
		name  string
		index *indexpb.SegmentIndex
		want  bool
	}{
		{
			name: "finished with memory validity artifacts",
			index: &indexpb.SegmentIndex{
				State:         commonpb.IndexState_Finished,
				IndexFileKeys: []string{"valid_data_count", "valid_data"},
			},
			want: true,
		},
		{
			name: "finished without validity artifacts",
			index: &indexpb.SegmentIndex{
				State: commonpb.IndexState_Finished,
			},
		},
		{
			name: "unfinished with validity artifacts",
			index: &indexpb.SegmentIndex{
				State:         commonpb.IndexState_InProgress,
				IndexFileKeys: []string{"valid_data_count", "valid_data"},
			},
		},
		{
			name: "malformed validity artifacts",
			index: &indexpb.SegmentIndex{
				State:         commonpb.IndexState_Finished,
				IndexFileKeys: []string{"valid_data_count"},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, vectorIndexNeedsObjectStore(test.index))
		})
	}
}

func TestBuildSegmentIndexRowsMismatchReport(t *testing.T) {
	report := &VectorIndexValidityReport{Records: []*VectorIndexValidityRecord{
		{
			SegmentID:     1,
			SegmentRows:   1000,
			ValidRows:     950,
			BitmapPresent: true,
			IndexState:    commonpb.IndexState_Finished.String(),
		},
		{
			SegmentID:     2,
			SegmentRows:   1000,
			ValidRows:     1000,
			BitmapPresent: true,
			IndexState:    commonpb.IndexState_Finished.String(),
		},
		{
			SegmentID:   3,
			SegmentRows: 1000,
			ValidRows:   0,
			IndexState:  commonpb.IndexState_Failed.String(),
		},
		{
			SegmentID:       4,
			SegmentRows:     1000,
			ValidRows:       900,
			BitmapPresent:   true,
			IndexState:      commonpb.IndexState_Finished.String(),
			InspectionError: "cannot read bitmap",
		},
		{
			SegmentID:   5,
			SegmentRows: 1000,
			ValidRows:   1000,
			IndexState:  commonpb.IndexState_Finished.String(),
			Status:      "INFERRED_ALL_VALID",
		},
	}}

	got := buildSegmentIndexRowsMismatchReport(report)
	require.Len(t, got.Records, 1)
	require.Equal(t, int64(1), got.UninspectedSegments)
	require.Equal(t, &SegmentIndexRowsMismatchRecord{
		SegmentID: 1, SegmentRows: 1000, ValidRows: 950,
	}, got.Records[0])
	require.Equal(t,
		"Vector index row mismatches: 1 segment(s)\n"+
			"Warning: 1 Finished segment index(es) could not be inspected\n"+
			"segment=1 segment_rows=1000 valid_rows=950",
		got.PrintAs(framework.FormatPlain))
	require.JSONEq(t,
		`{"records":[{"segment_id":1,"segment_rows":1000,"valid_rows":950}],"uninspected_segments":1}`,
		got.PrintAs(framework.FormatJSON))
	require.Equal(t,
		"{\"segment_id\":1,\"segment_rows\":1000,\"valid_rows\":950}\n"+
			`{"uninspected_segments":1}`,
		got.PrintAs(framework.FormatLine))
	require.Equal(t, table.Row{"Segment", "Segment rows", "Valid rows"}, got.TableHeaders())
	require.Equal(t, []table.Row{{int64(1), int64(1000), int64(950)}}, got.TableRows())
	require.Equal(t,
		"Vector index row mismatches: 1 segment(s), uninspected: 1",
		got.TableTitle())
}

func TestInspectVectorIndexMismatchOnlyRequiresField(t *testing.T) {
	_, err := (&InstanceState{}).InspectVectorIndexCommand(context.Background(), &InspectVectorIndexParams{
		CollectionID: 1,
		MismatchOnly: true,
	})
	require.ErrorContains(t, err, "mismatchOnly requires field")
}
