package states

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
)

type v3BytesReader []byte

func (r v3BytesReader) ReadRange(_ context.Context, _ string, offset, size int64) ([]byte, error) {
	if offset < 0 || size < 0 || offset+size > int64(len(r)) {
		return nil, fmt.Errorf("range out of bounds")
	}
	result := make([]byte, size)
	copy(result, r[offset:offset+size])
	return result, nil
}

type v3ReadRange struct {
	offset int64
	size   int64
}

type v3FooterReader struct {
	objectSize int64
	footer     []byte
	ranges     []v3ReadRange
}

func (r *v3FooterReader) ReadRange(_ context.Context, _ string, offset, size int64) ([]byte, error) {
	r.ranges = append(r.ranges, v3ReadRange{offset: offset, size: size})
	switch {
	case offset == 0 && size == v3IndexMagicSize:
		return []byte(v3IndexMagic), nil
	case offset == r.objectSize-v3IndexFooterSize && size == v3IndexFooterSize:
		return append([]byte(nil), r.footer...), nil
	default:
		return nil, fmt.Errorf("unexpected range read offset=%d size=%d", offset, size)
	}
}

func TestInspectV3IndexNumericSTLSort(t *testing.T) {
	meta := map[string]any{
		"index_type":   2,
		"index_length": 3,
		"is_nested":    false,
		"num_rows":     3,
	}
	data := buildPlainV3Index(t, make([]byte, 48), meta, false)

	inspection, findings := inspectV3Index(context.Background(), v3BytesReader(data), "numeric.v3", int64(len(data)))
	require.Empty(t, findings)
	require.Equal(t, uint16(3), inspection.FormatVersion)
	require.Equal(t, "HYBRID_STLSORT_NUMERIC", classifyV3PhysicalIndex(inspection.Meta))
	require.Equal(t, int64(48), inspection.IndexDataSize)
	require.Equal(t, float64(16), inspection.BytesPerEntry)
}

func TestInspectV3IndexStringWithoutIdxToOffsetsIsCompatible(t *testing.T) {
	meta := map[string]any{
		"index_type": 2,
		"is_nested":  false,
		"num_rows":   3,
		"version":    1,
	}
	data := buildPlainV3Index(t, []byte("index data"), meta, false)

	inspection, findings := inspectV3Index(context.Background(), v3BytesReader(data), "string.v3", int64(len(data)))
	require.Empty(t, findings)
	require.Equal(t, "HYBRID_STLSORT_STRING", classifyV3PhysicalIndex(inspection.Meta))
	require.NotContains(t, entryNames(inspection.Entries), "idx_to_offsets")
}

func TestClassifyV3StandaloneSTLSORTSignatures(t *testing.T) {
	require.Equal(t, "STLSORT_NUMERIC", classifyV3PhysicalIndex(map[string]any{
		"index_length": 10,
		"num_rows":     10,
	}, "STL_SORT"))
	require.Equal(t, "STLSORT_STRING", classifyV3PhysicalIndex(map[string]any{
		"version":  1,
		"num_rows": 10,
	}, "STLSORT"))
	require.Equal(t, "STLSORT_CONFLICT", classifyV3PhysicalIndex(map[string]any{
		"version":      1,
		"index_length": 10,
	}, "STL_SORT"))
	require.Equal(t, "NON_HYBRID_OR_UNKNOWN", classifyV3PhysicalIndex(map[string]any{
		"version": 1,
	}, "INVERTED"))
}

func TestInspectV3IndexCRCFailure(t *testing.T) {
	data := buildPlainV3Index(t, nil, map[string]any{"index_type": 1, "bitmap_index_length": 2}, true)

	_, findings := inspectV3Index(context.Background(), v3BytesReader(data), "crc.v3", int64(len(data)))
	requireFinding(t, findings, "ENTRY_CRC_MISMATCH")
}

func TestInspectV3IndexEncryptedIsNotMisreported(t *testing.T) {
	data := buildEncryptedV3Index(t)

	inspection, findings := inspectV3Index(context.Background(), v3BytesReader(data), "encrypted.v3", int64(len(data)))
	require.True(t, inspection.Encrypted)
	require.Equal(t, "zone-a", inspection.EncryptionZone)
	requireFinding(t, findings, "META_UNINSPECTED_ENCRYPTED")
	require.NotContains(t, findingRules(findings), "META_JSON_INVALID")
}

func TestInspectV3IndexRejectsOversizedFooterSectionsBeforeRead(t *testing.T) {
	tests := []struct {
		name          string
		metaSize      int64
		directorySize int64
		expectedText  string
	}{
		{
			name:          "directory",
			metaSize:      1,
			directorySize: v3MaxDirectoryTableSize + 1,
			expectedText:  "directory table size",
		},
		{
			name:          "meta",
			metaSize:      v3MaxMetaEntrySize + 1,
			directorySize: 1,
			expectedText:  "meta entry size",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			footer := make([]byte, v3IndexFooterSize)
			binary.LittleEndian.PutUint16(footer[0:2], 3)
			binary.LittleEndian.PutUint32(footer[24:28], uint32(test.metaSize))
			binary.LittleEndian.PutUint32(footer[28:32], uint32(test.directorySize))
			objectSize := v3IndexMagicSize + test.metaSize + test.directorySize + v3IndexFooterSize
			reader := &v3FooterReader{objectSize: objectSize, footer: footer}

			_, findings := inspectV3Index(context.Background(), reader, "oversized.v3", objectSize)

			requireFinding(t, findings, "V3_FOOTER_INVALID")
			require.Contains(t, findings[0].Message, test.expectedText)
			require.Equal(t, []v3ReadRange{
				{offset: 0, size: v3IndexMagicSize},
				{offset: objectSize - v3IndexFooterSize, size: v3IndexFooterSize},
			}, reader.ranges)
		})
	}
}

func TestChooseV3SegmentIndexPrefersMatchingCandidateWhenAmbiguous(t *testing.T) {
	objectPath := v3ObjectPath{
		layout:       "BUILD_ROOTED",
		partitionID:  10,
		segmentID:    20,
		indexVersion: 1,
	}
	nonMatching := &indexpb.SegmentIndex{
		CollectionID: 100,
		PartitionID:  99,
		SegmentID:    99,
		IndexID:      1,
		IndexVersion: 1,
	}
	firstMatching := &indexpb.SegmentIndex{
		CollectionID: 100,
		PartitionID:  10,
		SegmentID:    20,
		IndexID:      2,
		IndexVersion: 1,
	}
	secondMatching := &indexpb.SegmentIndex{
		CollectionID: 100,
		PartitionID:  10,
		SegmentID:    20,
		IndexID:      3,
		IndexVersion: 1,
	}

	selected, ambiguous := chooseV3SegmentIndex(objectPath, []*indexpb.SegmentIndex{
		nonMatching,
		firstMatching,
		secondMatching,
	})

	require.Same(t, firstMatching, selected)
	require.True(t, ambiguous)
}

func TestCheckV3MetaCompatibilityFindsLoaderMismatches(t *testing.T) {
	tests := []struct {
		name         string
		effective    schemapb.DataType
		meta         map[string]any
		physical     string
		expectedRule string
	}{
		{
			name:         "string field with numeric stlsort",
			effective:    schemapb.DataType_VarChar,
			meta:         map[string]any{"index_type": 2, "index_length": 10, "num_rows": 10},
			physical:     "HYBRID_STLSORT_NUMERIC",
			expectedRule: "STRING_FIELD_NUMERIC_STLSORT",
		},
		{
			name:         "numeric field with string stlsort",
			effective:    schemapb.DataType_Int64,
			meta:         map[string]any{"index_type": 2, "version": 1, "num_rows": 10},
			physical:     "HYBRID_STLSORT_STRING",
			expectedRule: "NUMERIC_FIELD_STRING_STLSORT",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			record := &V3IndexScanRecord{
				Meta:               test.meta,
				PhysicalIndexType:  test.physical,
				EffectiveFieldType: test.effective.String(),
				effectiveType:      test.effective,
				effectiveTypeKnown: true,
				Findings:           make([]V3IndexFinding, 0),
			}
			checkV3MetaCompatibility(record, &indexpb.SegmentIndex{NumRows: 10}, nil)
			requireFinding(t, record.Findings, test.expectedRule)
		})
	}
}

func TestNumericSTLSortWithoutVersionIsValidForNumericField(t *testing.T) {
	record := &V3IndexScanRecord{
		Meta:               map[string]any{"index_type": 2, "index_length": 10, "num_rows": 10},
		PhysicalIndexType:  "HYBRID_STLSORT_NUMERIC",
		EffectiveFieldType: schemapb.DataType_Int64.String(),
		effectiveType:      schemapb.DataType_Int64,
		effectiveTypeKnown: true,
		Findings:           make([]V3IndexFinding, 0),
	}
	checkV3MetaCompatibility(record, &indexpb.SegmentIndex{NumRows: 10}, nil)
	require.Empty(t, record.Findings)
}

func TestCheckV3MetaCompatibilityFindsJSONPathLoaderMismatches(t *testing.T) {
	tests := []struct {
		name         string
		castType     string
		effective    schemapb.DataType
		meta         map[string]any
		physical     string
		expectedRule string
	}{
		{
			name:         "varchar path with standalone numeric stlsort",
			castType:     "VARCHAR",
			effective:    schemapb.DataType_VarChar,
			meta:         map[string]any{"index_length": 10, "num_rows": 10},
			physical:     "STLSORT_NUMERIC",
			expectedRule: "JSON_PATH_VARCHAR_NUMERIC_STLSORT",
		},
		{
			name:         "double path with hybrid string stlsort",
			castType:     "DOUBLE",
			effective:    schemapb.DataType_Double,
			meta:         map[string]any{"index_type": 2, "version": 1, "num_rows": 10},
			physical:     "HYBRID_STLSORT_STRING",
			expectedRule: "JSON_PATH_NUMERIC_STRING_STLSORT",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			record := &V3IndexScanRecord{
				Meta:               test.meta,
				PhysicalIndexType:  test.physical,
				MetadataIndexType:  "STL_SORT",
				EffectiveFieldType: test.effective.String(),
				JSONPath:           "/payload/value",
				JSONCastType:       test.castType,
				IsJSONPathIndex:    true,
				effectiveType:      test.effective,
				effectiveTypeKnown: true,
				isJSONPathIndex:    true,
				Findings:           make([]V3IndexFinding, 0),
			}
			checkV3MetaCompatibility(record, &indexpb.SegmentIndex{NumRows: 10}, nil)
			requireFinding(t, record.Findings, test.expectedRule)
		})
	}
}

func TestParseV3ObjectPathLayouts(t *testing.T) {
	buildRooted, err := parseV3ObjectPath("files", "files/index_files/123/1/10/20/milvus_packed_hybrid_index.v3")
	require.NoError(t, err)
	require.Equal(t, "BUILD_ROOTED", buildRooted.layout)
	require.Equal(t, int64(123), buildRooted.buildID)
	require.Equal(t, int64(20), buildRooted.segmentID)

	collectionRooted, err := parseV3ObjectPath("files", "files/index_v1/100/10/20/123/1/milvus_packed_hybrid_index.v3")
	require.NoError(t, err)
	require.Equal(t, "COLLECTION_ROOTED", collectionRooted.layout)
	require.Equal(t, int64(100), collectionRooted.collectionID)
	require.Equal(t, int64(123), collectionRooted.buildID)
}

func TestSegmentIndexPathVersionReadsForwardCompatibleUnknownField(t *testing.T) {
	segmentIndex := &indexpb.SegmentIndex{}
	unknown := protowire.AppendTag(nil, 22, protowire.VarintType)
	unknown = protowire.AppendVarint(unknown, 1)
	segmentIndex.ProtoReflect().SetUnknown(unknown)

	require.Equal(t, int32(1), v3SegmentIndexPathVersion(segmentIndex))
}

func TestResolveV3EffectiveField(t *testing.T) {
	collection := models.NewCollection(&etcdpb.CollectionInfo{
		ID: 100,
		Schema: &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{FieldID: 101, Name: "tags", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_VarChar},
				{FieldID: 102, Name: "payload", DataType: schemapb.DataType_JSON},
			},
			StructArrayFields: []*schemapb.StructArrayFieldSchema{
				{
					FieldID: 200,
					Name:    "objects",
					Fields: []*schemapb.FieldSchema{
						{FieldID: 201, Name: "object_id", DataType: schemapb.DataType_Int64},
					},
				},
			},
		},
	}, "")

	arrayField := resolveV3EffectiveField(collection, &indexpb.IndexInfo{FieldID: 101})
	require.True(t, arrayField.known)
	require.Equal(t, schemapb.DataType_VarChar, arrayField.effectiveType)

	jsonField := resolveV3EffectiveField(collection, &indexpb.IndexInfo{
		FieldID: 102,
		IndexParams: []*commonpb.KeyValuePair{
			{Key: "json_path", Value: "payload['ts']"},
			{Key: "json_cast_type", Value: "DOUBLE"},
		},
	})
	require.True(t, jsonField.known)
	require.True(t, jsonField.isJSONPathIndex)
	require.Equal(t, schemapb.DataType_Double, jsonField.effectiveType)
	require.Equal(t, "payload['ts']", jsonField.jsonPath)
	require.Equal(t, "DOUBLE", jsonField.jsonCastType)

	arrayJSONField := resolveV3EffectiveField(collection, &indexpb.IndexInfo{
		FieldID: 102,
		IndexParams: []*commonpb.KeyValuePair{
			{Key: "json_path", Value: "payload['tags']"},
			{Key: "json_cast_type", Value: "ARRAY_VARCHAR"},
		},
	})
	require.True(t, arrayJSONField.known)
	require.Equal(t, schemapb.DataType_VarChar, arrayJSONField.effectiveType)

	nestedField := resolveV3EffectiveField(collection, &indexpb.IndexInfo{FieldID: 201})
	require.True(t, nestedField.known)
	require.Equal(t, schemapb.DataType_Int64, nestedField.effectiveType)
	require.Equal(t, "objects[object_id]", nestedField.name)
}

func TestExpectedV3JSONLoader(t *testing.T) {
	require.Equal(t,
		"JsonHybridScalarIndex<std::string>",
		expectedV3JSONLoader("HYBRID", schemapb.DataType_VarChar, true))
	require.Equal(t,
		"JsonScalarIndexWrapper<std::string, StringIndexSort>",
		expectedV3JSONLoader("STL_SORT", schemapb.DataType_VarChar, true))
	require.Equal(t,
		"JsonScalarIndexWrapper<double, ScalarIndexSort<double>>",
		expectedV3JSONLoader("STL_SORT", schemapb.DataType_Double, true))
}

func buildPlainV3Index(t *testing.T, indexData []byte, meta map[string]any, corruptCRC bool) []byte {
	t.Helper()
	metaRaw, err := json.Marshal(meta)
	require.NoError(t, err)
	checksum := crc32.Checksum(metaRaw, crc32.MakeTable(crc32.Castagnoli))
	if corruptCRC {
		checksum++
	}
	entries := make([]map[string]any, 0, 2)
	if len(indexData) > 0 {
		entries = append(entries, map[string]any{
			"name":   "index_data",
			"offset": 0,
			"size":   len(indexData),
			"crc32":  "00000000",
		})
	}
	entries = append(entries, map[string]any{
		"name":   "__meta__",
		"offset": len(indexData),
		"size":   len(metaRaw),
		"crc32":  fmt.Sprintf("%08X", checksum),
	})
	directoryRaw, err := json.Marshal(map[string]any{"entries": entries})
	require.NoError(t, err)
	footer := make([]byte, v3IndexFooterSize)
	binary.LittleEndian.PutUint16(footer[0:2], 3)
	binary.LittleEndian.PutUint32(footer[24:28], uint32(len(metaRaw)))
	binary.LittleEndian.PutUint32(footer[28:32], uint32(len(directoryRaw)))

	result := make([]byte, 0, len(v3IndexMagic)+len(indexData)+len(metaRaw)+len(directoryRaw)+len(footer))
	result = append(result, v3IndexMagic...)
	result = append(result, indexData...)
	result = append(result, metaRaw...)
	result = append(result, directoryRaw...)
	result = append(result, footer...)
	return result
}

func buildEncryptedV3Index(t *testing.T) []byte {
	t.Helper()
	ciphertext := []byte("ciphertext")
	directoryRaw, err := json.Marshal(map[string]any{
		"slice_size": 4194304,
		"entries": []map[string]any{
			{
				"name":          "__meta__",
				"original_size": 42,
				"crc32":         "00000000",
				"slices": []map[string]any{
					{"offset": 0, "size": len(ciphertext)},
				},
			},
		},
		"__edek__":  "edek",
		"__ez_id__": "zone-a",
	})
	require.NoError(t, err)
	footer := make([]byte, v3IndexFooterSize)
	binary.LittleEndian.PutUint16(footer[0:2], 3)
	binary.LittleEndian.PutUint32(footer[24:28], 42)
	binary.LittleEndian.PutUint32(footer[28:32], uint32(len(directoryRaw)))

	result := append([]byte(v3IndexMagic), ciphertext...)
	result = append(result, directoryRaw...)
	result = append(result, footer...)
	return result
}

func entryNames(entries []v3DirectoryEntry) []string {
	result := make([]string, 0, len(entries))
	for _, entry := range entries {
		result = append(result, entry.Name)
	}
	return result
}

func findingRules(findings []V3IndexFinding) []string {
	result := make([]string, 0, len(findings))
	for _, finding := range findings {
		result = append(result, finding.Rule)
	}
	return result
}

func requireFinding(t *testing.T, findings []V3IndexFinding, rule string) {
	t.Helper()
	require.Contains(t, findingRules(findings), rule)
}
