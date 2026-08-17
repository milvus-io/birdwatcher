package states

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"strconv"
	"strings"

	"github.com/milvus-io/birdwatcher/oss"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

const (
	v3IndexMagic            = "MVSIDXV3"
	v3IndexMagicSize        = int64(8)
	v3IndexFooterSize       = int64(32)
	v3MaxDirectoryTableSize = int64(16 * 1024 * 1024)
	v3MaxMetaEntrySize      = int64(64 * 1024 * 1024)
)

const (
	v3SeverityCritical = "Critical"
	v3SeverityError    = "Error"
	v3SeverityWarning  = "Warning"
	v3SeverityInfo     = "Info"
)

type V3IndexFinding struct {
	Rule     string `json:"rule"`
	Severity string `json:"severity"`
	Message  string `json:"message"`
}

type v3DirectoryEntry struct {
	Name         string    `json:"name"`
	Offset       int64     `json:"offset"`
	Size         int64     `json:"size"`
	OriginalSize int64     `json:"original_size"`
	CRC32        string    `json:"crc32"`
	Slices       []v3Slice `json:"slices"`
}

type v3Slice struct {
	Offset int64 `json:"offset"`
	Size   int64 `json:"size"`
}

type v3Directory struct {
	SliceSize int64              `json:"slice_size"`
	Entries   []v3DirectoryEntry `json:"entries"`
}

type v3IndexInspection struct {
	FormatVersion  uint16
	Encrypted      bool
	EncryptionZone string
	Meta           map[string]any
	Entries        []v3DirectoryEntry
	IndexDataSize  int64
	BytesPerEntry  float64
}

type v3RangeReader interface {
	ReadRange(ctx context.Context, key string, offset, size int64) ([]byte, error)
}

type objectStoreV3RangeReader struct {
	store oss.ObjectStore
}

func (r objectStoreV3RangeReader) ReadRange(ctx context.Context, key string, offset, size int64) ([]byte, error) {
	if offset < 0 || size <= 0 {
		return nil, fmt.Errorf("invalid object range offset=%d size=%d", offset, size)
	}
	obj, err := r.store.Open(ctx, key, oss.WithOpenRange(offset, offset+size-1))
	if err != nil {
		return nil, err
	}
	if closer, ok := obj.(io.Closer); ok {
		defer closer.Close()
	}
	data, err := io.ReadAll(obj)
	if err != nil {
		return nil, err
	}
	if int64(len(data)) != size {
		return nil, fmt.Errorf("short range read: expected %d bytes, got %d", size, len(data))
	}
	return data, nil
}

func inspectV3Index(ctx context.Context, reader v3RangeReader, key string, objectSize int64) (v3IndexInspection, []V3IndexFinding) {
	inspection := v3IndexInspection{}
	if objectSize < v3IndexMagicSize+v3IndexFooterSize {
		return inspection, []V3IndexFinding{{
			Rule:     "V3_FOOTER_INVALID",
			Severity: v3SeverityCritical,
			Message:  fmt.Sprintf("object is too small for a V3 index: %d bytes", objectSize),
		}}
	}

	magic, err := reader.ReadRange(ctx, key, 0, v3IndexMagicSize)
	if err != nil {
		return inspection, objectReadFinding("magic", err)
	}
	if string(magic) != v3IndexMagic {
		return inspection, []V3IndexFinding{{
			Rule:     "V3_MAGIC_INVALID",
			Severity: v3SeverityCritical,
			Message:  fmt.Sprintf("expected magic %q, got %s", v3IndexMagic, hex.EncodeToString(magic)),
		}}
	}

	footer, err := reader.ReadRange(ctx, key, objectSize-v3IndexFooterSize, v3IndexFooterSize)
	if err != nil {
		return inspection, objectReadFinding("footer", err)
	}
	inspection.FormatVersion = binary.LittleEndian.Uint16(footer[0:2])
	metaSize := int64(binary.LittleEndian.Uint32(footer[24:28]))
	directorySize := int64(binary.LittleEndian.Uint32(footer[28:32]))
	if inspection.FormatVersion != 3 {
		return inspection, []V3IndexFinding{{
			Rule:     "V3_FOOTER_INVALID",
			Severity: v3SeverityCritical,
			Message:  fmt.Sprintf("unsupported V3 footer format version %d", inspection.FormatVersion),
		}}
	}
	if directorySize <= 0 {
		return inspection, []V3IndexFinding{{
			Rule:     "V3_FOOTER_INVALID",
			Severity: v3SeverityCritical,
			Message:  "directory table size is empty",
		}}
	}
	if directorySize > v3MaxDirectoryTableSize {
		return inspection, []V3IndexFinding{{
			Rule:     "V3_FOOTER_INVALID",
			Severity: v3SeverityCritical,
			Message: fmt.Sprintf("directory table size %d exceeds scanner limit %d",
				directorySize, v3MaxDirectoryTableSize),
		}}
	}
	if metaSize > v3MaxMetaEntrySize {
		return inspection, []V3IndexFinding{{
			Rule:     "V3_FOOTER_INVALID",
			Severity: v3SeverityCritical,
			Message: fmt.Sprintf("meta entry size %d exceeds scanner limit %d",
				metaSize, v3MaxMetaEntrySize),
		}}
	}
	directoryOffset := objectSize - v3IndexFooterSize - directorySize
	if directoryOffset < v3IndexMagicSize {
		return inspection, []V3IndexFinding{{
			Rule:     "V3_FOOTER_INVALID",
			Severity: v3SeverityCritical,
			Message:  fmt.Sprintf("directory offset %d is outside the data region", directoryOffset),
		}}
	}

	directoryRaw, err := reader.ReadRange(ctx, key, directoryOffset, directorySize)
	if err != nil {
		return inspection, objectReadFinding("directory table", err)
	}
	var directoryKeys map[string]json.RawMessage
	if err := json.Unmarshal(directoryRaw, &directoryKeys); err != nil {
		return inspection, []V3IndexFinding{{
			Rule:     "DIRECTORY_JSON_INVALID",
			Severity: v3SeverityCritical,
			Message:  fmt.Sprintf("cannot decode directory table JSON: %v", err),
		}}
	}
	var directory v3Directory
	if err := json.Unmarshal(directoryRaw, &directory); err != nil {
		return inspection, []V3IndexFinding{{
			Rule:     "DIRECTORY_JSON_INVALID",
			Severity: v3SeverityCritical,
			Message:  fmt.Sprintf("cannot decode directory entries: %v", err),
		}}
	}
	inspection.Entries = directory.Entries
	if _, ok := directoryKeys["__edek__"]; ok {
		inspection.Encrypted = true
		if raw, ok := directoryKeys["__ez_id__"]; ok {
			_ = json.Unmarshal(raw, &inspection.EncryptionZone)
		}
		return inspection, []V3IndexFinding{{
			Rule:     "META_UNINSPECTED_ENCRYPTED",
			Severity: v3SeverityInfo,
			Message:  "V3 directory is encrypted; __meta__ was not decoded without the Milvus cipher plugin",
		}}
	}

	var metaEntry *v3DirectoryEntry
	for i := range directory.Entries {
		entry := &directory.Entries[i]
		if entry.Name == "index_data" {
			inspection.IndexDataSize = entry.Size
		}
		if entry.Name == "__meta__" {
			metaEntry = entry
		}
	}
	if metaEntry == nil {
		return inspection, []V3IndexFinding{{
			Rule:     "META_ENTRY_MISSING",
			Severity: v3SeverityCritical,
			Message:  "directory table does not contain __meta__",
		}}
	}
	if metaEntry.Offset < 0 || metaEntry.Size <= 0 {
		return inspection, []V3IndexFinding{{
			Rule:     "V3_FOOTER_INVALID",
			Severity: v3SeverityCritical,
			Message:  fmt.Sprintf("invalid __meta__ entry range offset=%d size=%d", metaEntry.Offset, metaEntry.Size),
		}}
	}
	if metaEntry.Size > v3MaxMetaEntrySize {
		return inspection, []V3IndexFinding{{
			Rule:     "V3_FOOTER_INVALID",
			Severity: v3SeverityCritical,
			Message: fmt.Sprintf("__meta__ entry size %d exceeds scanner limit %d",
				metaEntry.Size, v3MaxMetaEntrySize),
		}}
	}
	if metaEntry.Offset > directoryOffset-v3IndexMagicSize {
		return inspection, []V3IndexFinding{{
			Rule:     "V3_FOOTER_INVALID",
			Severity: v3SeverityCritical,
			Message:  fmt.Sprintf("__meta__ offset %d is outside the data region", metaEntry.Offset),
		}}
	}
	metaOffset := v3IndexMagicSize + metaEntry.Offset
	if metaEntry.Size > directoryOffset-metaOffset {
		return inspection, []V3IndexFinding{{
			Rule:     "V3_FOOTER_INVALID",
			Severity: v3SeverityCritical,
			Message: fmt.Sprintf("__meta__ range at offset %d with size %d overlaps or exceeds the directory table",
				metaOffset, metaEntry.Size),
		}}
	}
	if metaSize != metaEntry.Size {
		return inspection, []V3IndexFinding{{
			Rule:     "V3_FOOTER_INVALID",
			Severity: v3SeverityCritical,
			Message:  fmt.Sprintf("footer meta size %d differs from directory meta size %d", metaSize, metaEntry.Size),
		}}
	}

	metaRaw, err := reader.ReadRange(ctx, key, metaOffset, metaEntry.Size)
	if err != nil {
		return inspection, objectReadFinding("__meta__", err)
	}
	findings := make([]V3IndexFinding, 0, 1)
	if metaEntry.CRC32 != "" {
		expected, err := strconv.ParseUint(strings.TrimPrefix(strings.TrimPrefix(metaEntry.CRC32, "0x"), "0X"), 16, 32)
		if err != nil {
			findings = append(findings, V3IndexFinding{
				Rule:     "DIRECTORY_JSON_INVALID",
				Severity: v3SeverityCritical,
				Message:  fmt.Sprintf("invalid __meta__ CRC32C %q: %v", metaEntry.CRC32, err),
			})
		} else {
			actual := crc32.Checksum(metaRaw, crc32.MakeTable(crc32.Castagnoli))
			if actual != uint32(expected) {
				findings = append(findings, V3IndexFinding{
					Rule:     "ENTRY_CRC_MISMATCH",
					Severity: v3SeverityCritical,
					Message:  fmt.Sprintf("__meta__ CRC32C expected %08X, got %08X", uint32(expected), actual),
				})
			}
		}
	}
	decoder := json.NewDecoder(bytes.NewReader(metaRaw))
	decoder.UseNumber()
	if err := decoder.Decode(&inspection.Meta); err != nil {
		findings = append(findings, V3IndexFinding{
			Rule:     "META_JSON_INVALID",
			Severity: v3SeverityCritical,
			Message:  fmt.Sprintf("cannot decode plaintext __meta__ JSON: %v", err),
		})
		return inspection, findings
	}
	if inspection.Meta == nil {
		findings = append(findings, V3IndexFinding{
			Rule:     "META_JSON_INVALID",
			Severity: v3SeverityCritical,
			Message:  "plaintext __meta__ must be a JSON object",
		})
		return inspection, findings
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		findings = append(findings, V3IndexFinding{
			Rule:     "META_JSON_INVALID",
			Severity: v3SeverityCritical,
			Message:  "plaintext __meta__ contains trailing JSON data",
		})
		return inspection, findings
	}
	if indexLength, ok := v3MetaInt64(inspection.Meta, "index_length"); ok && indexLength > 0 && inspection.IndexDataSize > 0 {
		inspection.BytesPerEntry = float64(inspection.IndexDataSize) / float64(indexLength)
	}
	return inspection, findings
}

func objectReadFinding(part string, err error) []V3IndexFinding {
	return []V3IndexFinding{{
		Rule:     "OBJECT_READ_FAILED",
		Severity: v3SeverityCritical,
		Message:  fmt.Sprintf("cannot read V3 %s: %v", part, err),
	}}
}

func classifyV3PhysicalIndex(meta map[string]any, metadataIndexType ...string) string {
	if meta == nil {
		return "UNKNOWN"
	}
	internalType, hasInternalType := v3MetaInt64(meta, "index_type")
	_, hasBitmapLength := meta["bitmap_index_length"]
	_, hasVersion := meta["version"]
	_, hasIndexLength := meta["index_length"]
	_, hasFileNames := meta["file_names"]

	switch {
	case internalType == 1 && hasBitmapLength:
		return "HYBRID_BITMAP"
	case internalType == 2 && hasVersion && !hasIndexLength:
		return "HYBRID_STLSORT_STRING"
	case internalType == 2 && hasIndexLength && !hasVersion:
		return "HYBRID_STLSORT_NUMERIC"
	case internalType == 2 && hasVersion && hasIndexLength:
		return "HYBRID_STLSORT_CONFLICT"
	case internalType == 4 && hasFileNames:
		return "HYBRID_INVERTED"
	case hasInternalType:
		return "HYBRID_UNKNOWN_INTERNAL_FORMAT"
	case len(metadataIndexType) > 0 && !isV3STLSORTIndexType(metadataIndexType[0]):
		return "NON_HYBRID_OR_UNKNOWN"
	case hasVersion && !hasIndexLength:
		return "STLSORT_STRING"
	case hasIndexLength && !hasVersion:
		return "STLSORT_NUMERIC"
	case hasVersion && hasIndexLength:
		return "STLSORT_CONFLICT"
	default:
		return "NON_HYBRID_OR_UNKNOWN"
	}
}

func isV3STLSORTIndexType(indexType string) bool {
	normalized := strings.ToUpper(strings.TrimSpace(indexType))
	normalized = strings.ReplaceAll(normalized, "_", "")
	return normalized == "STLSORT"
}

func v3MetaInt64(meta map[string]any, key string) (int64, bool) {
	value, ok := meta[key]
	if !ok {
		return 0, false
	}
	switch typed := value.(type) {
	case json.Number:
		result, err := typed.Int64()
		return result, err == nil
	case float64:
		return int64(typed), true
	case int:
		return int64(typed), true
	case int32:
		return int64(typed), true
	case int64:
		return typed, true
	case uint32:
		return int64(typed), true
	case uint64:
		if typed > math.MaxInt64 {
			return 0, false
		}
		return int64(typed), true
	default:
		return 0, false
	}
}

func isV3StringType(dataType schemapb.DataType) bool {
	switch dataType {
	case schemapb.DataType_String, schemapb.DataType_VarChar, schemapb.DataType_Text:
		return true
	default:
		return false
	}
}

func isV3NumericType(dataType schemapb.DataType) bool {
	switch dataType {
	case schemapb.DataType_Int8,
		schemapb.DataType_Int16,
		schemapb.DataType_Int32,
		schemapb.DataType_Int64,
		schemapb.DataType_Float,
		schemapb.DataType_Double,
		schemapb.DataType_Timestamptz:
		return true
	default:
		return false
	}
}

func expectedV3Loader(dataType schemapb.DataType, known bool) string {
	if !known {
		return "UNKNOWN"
	}
	if isV3StringType(dataType) {
		return "StringIndexSort"
	}
	cppType := map[schemapb.DataType]string{
		schemapb.DataType_Int8:        "int8_t",
		schemapb.DataType_Int16:       "int16_t",
		schemapb.DataType_Int32:       "int32_t",
		schemapb.DataType_Int64:       "int64_t",
		schemapb.DataType_Float:       "float",
		schemapb.DataType_Double:      "double",
		schemapb.DataType_Timestamptz: "int64_t",
	}
	if name, ok := cppType[dataType]; ok {
		return fmt.Sprintf("ScalarIndexSort<%s>", name)
	}
	return dataType.String()
}

func expectedV3JSONLoader(indexType string, dataType schemapb.DataType, known bool) string {
	if !known {
		return "UNKNOWN"
	}
	cppType := map[schemapb.DataType]string{
		schemapb.DataType_Bool:    "bool",
		schemapb.DataType_Double:  "double",
		schemapb.DataType_VarChar: "std::string",
	}
	typeName, ok := cppType[dataType]
	if !ok && dataType != schemapb.DataType_JSON {
		return "UNKNOWN"
	}

	switch strings.ToUpper(strings.TrimSpace(indexType)) {
	case "HYBRID":
		if dataType == schemapb.DataType_JSON {
			return "UNSUPPORTED_JSON_HYBRID"
		}
		return fmt.Sprintf("JsonHybridScalarIndex<%s>", typeName)
	case "STL_SORT", "STLSORT":
		base := map[schemapb.DataType]string{
			schemapb.DataType_Double:  "ScalarIndexSort<double>",
			schemapb.DataType_VarChar: "StringIndexSort",
		}[dataType]
		if base == "" {
			return "UNSUPPORTED_JSON_STL_SORT"
		}
		return fmt.Sprintf("JsonScalarIndexWrapper<%s, %s>", typeName, base)
	case "BITMAP":
		if dataType == schemapb.DataType_JSON {
			return "UNSUPPORTED_JSON_BITMAP"
		}
		return fmt.Sprintf("JsonScalarIndexWrapper<%s, BitmapIndex<%s>>", typeName, typeName)
	case "INVERTED":
		if dataType == schemapb.DataType_JSON {
			return "JsonFlatIndex"
		}
		return fmt.Sprintf("JsonScalarIndexWrapper<%s, InvertedIndexTantivy<%s>>", typeName, typeName)
	default:
		if dataType == schemapb.DataType_JSON {
			return "UNKNOWN"
		}
		return fmt.Sprintf("JsonScalarIndexWrapper<%s>", typeName)
	}
}
