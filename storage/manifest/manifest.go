// Package manifest reads Milvus storage v3 manifests without native dependencies.
package manifest

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/hamba/avro/v2"
	"github.com/hamba/avro/v2/ocf"
)

// Manifest parsing types and logic (Avro binary encoding)

const Magic int32 = 0x4D494C56 // "MILV"

// Manifest format evolution:
//   - v1: initial (column_groups, delta_logs, stats as map<string, array<string>>)
//   - v2: added indexes
//   - v3: stats changed to map<string, Statistics>
//   - v4: ColumnGroupFile.metadata (bytes) replaced by properties (map<string,string>)
//   - v5: added lob_files
//   - v6: expanded Index with typed artifact metadata
const (
	VersionV3 int32 = 3
	VersionV4 int32 = 4
	VersionV5 int32 = 5
	VersionV6 int32 = 6
)

// avroOCFMagic is the 4-byte magic header for Avro Object Container Format files.
var avroOCFMagic = []byte{'O', 'b', 'j', 0x01}

type ColumnGroupFile struct {
	Path       string            `json:"path" avro:"path"`
	StartIndex int64             `json:"start_index" avro:"start_index"`
	EndIndex   int64             `json:"end_index" avro:"end_index"`
	Metadata   []byte            `json:"metadata,omitempty" avro:"metadata"`
	Properties map[string]string `json:"properties,omitempty" avro:"properties"`
}

type ColumnGroup struct {
	Columns []string          `json:"columns" avro:"columns"`
	Files   []ColumnGroupFile `json:"files" avro:"files"`
	Format  string            `json:"format" avro:"format"`
}

type DeltaLogType int32

const (
	DeltaLogTypePrimaryKey DeltaLogType = 0
	DeltaLogTypePositional DeltaLogType = 1
	DeltaLogTypeEquality   DeltaLogType = 2
)

func (t DeltaLogType) String() string {
	switch t {
	case DeltaLogTypePrimaryKey:
		return "PRIMARY_KEY"
	case DeltaLogTypePositional:
		return "POSITIONAL"
	case DeltaLogTypeEquality:
		return "EQUALITY"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", int32(t))
	}
}

func (t DeltaLogType) MarshalJSON() ([]byte, error) {
	return json.Marshal(t.String())
}

type DeltaLog struct {
	Path       string       `json:"path" avro:"path"`
	Type       DeltaLogType `json:"type" avro:"type"`
	NumEntries int64        `json:"num_entries" avro:"num_entries"`
}

// Index represents index metadata for a column.
type Index struct {
	ColumnName                string            `json:"column_name" avro:"column_name"`
	IndexName                 string            `json:"index_name,omitempty" avro:"index_name"`
	IndexType                 string            `json:"index_type" avro:"index_type"`
	Path                      string            `json:"path" avro:"path"`
	FieldID                   int64             `json:"field_id,omitempty" avro:"field_id"`
	IndexID                   int64             `json:"index_id,omitempty" avro:"index_id"`
	BuildID                   int64             `json:"build_id,omitempty" avro:"build_id"`
	IndexVersion              int64             `json:"index_version,omitempty" avro:"index_version"`
	NumRows                   int64             `json:"num_rows,omitempty" avro:"num_rows"`
	SerializedSize            int64             `json:"serialized_size,omitempty" avro:"serialized_size"`
	MemSize                   int64             `json:"mem_size,omitempty" avro:"mem_size"`
	CurrentIndexVersion       int32             `json:"current_index_version,omitempty" avro:"current_index_version"`
	CurrentScalarIndexVersion int32             `json:"current_scalar_index_version,omitempty" avro:"current_scalar_index_version"`
	IndexStorePathVersion     int32             `json:"index_store_path_version,omitempty" avro:"index_store_path_version"`
	IndexFileKeys             []string          `json:"index_file_keys,omitempty" avro:"index_file_keys"`
	Properties                map[string]string `json:"properties,omitempty" avro:"properties"`
}

type LobFileInfo struct {
	Path          string `json:"path" avro:"path"`
	FieldID       int64  `json:"field_id" avro:"field_id"`
	TotalRows     int64  `json:"total_rows" avro:"total_rows"`
	ValidRows     int64  `json:"valid_rows" avro:"valid_rows"`
	FileSizeBytes int64  `json:"file_size_bytes" avro:"file_size_bytes"`
}

// Statistics represents a stats entry with file paths and optional metadata.
type Statistics struct {
	Paths    []string          `json:"paths" avro:"paths"`
	Metadata map[string]string `json:"metadata,omitempty" avro:"metadata"`
}

type Manifest struct {
	Format       string                 `json:"format" avro:"format"`
	Magic        int32                  `json:"magic,omitempty" avro:"magic"`
	MagicStr     string                 `json:"magic_str,omitempty" avro:"magic_str"`
	Version      int32                  `json:"version" avro:"version"`
	ColumnGroups []ColumnGroup          `json:"column_groups" avro:"column_groups"`
	DeltaLogs    []DeltaLog             `json:"delta_logs" avro:"delta_logs"`
	Stats        map[string]*Statistics `json:"stats" avro:"stats"`
	Indexes      []Index                `json:"indexes,omitempty" avro:"indexes"`
	LobFiles     []LobFileInfo          `json:"lob_files,omitempty" avro:"lob_files"`
}

// avroReader wraps an io.Reader to decode Avro binary encoding primitives.
type avroReader struct {
	r io.Reader
}

func (a *avroReader) readByte() (byte, error) {
	var buf [1]byte
	_, err := io.ReadFull(a.r, buf[:])
	return buf[0], err
}

func (a *avroReader) readLong() (int64, error) {
	var val uint64
	var shift uint
	for {
		b, err := a.readByte()
		if err != nil {
			if shift != 0 && errors.Is(err, io.EOF) {
				return 0, io.ErrUnexpectedEOF
			}
			return 0, fmt.Errorf("readLong: %w", err)
		}
		if shift > 63 || (shift == 63 && b > 1) {
			return 0, fmt.Errorf("avro long overflows int64")
		}
		val |= uint64(b&0x7f) << shift
		if b&0x80 == 0 {
			break
		}
		shift += 7
	}
	return int64(val>>1) ^ -int64(val&1), nil
}

func (a *avroReader) readInt() (int32, error) {
	v, err := a.readLong()
	if err != nil {
		return 0, err
	}
	return int32(v), nil
}

func (a *avroReader) readString() (string, error) {
	length, err := a.readLong()
	if err != nil {
		return "", fmt.Errorf("readString length: %w", err)
	}
	if length < 0 {
		return "", fmt.Errorf("readString: negative length %d", length)
	}
	if length == 0 {
		return "", nil
	}
	if remaining, ok := a.r.(interface{ Len() int }); ok && length > int64(remaining.Len()) {
		return "", io.ErrUnexpectedEOF
	}
	buf := make([]byte, length)
	_, err = io.ReadFull(a.r, buf)
	if err != nil {
		return "", fmt.Errorf("readString data: %w", err)
	}
	return string(buf), nil
}

func (a *avroReader) readBytes() ([]byte, error) {
	length, err := a.readLong()
	if err != nil {
		return nil, fmt.Errorf("readBytes length: %w", err)
	}
	if length < 0 {
		return nil, fmt.Errorf("readBytes: negative length %d", length)
	}
	if length == 0 {
		return nil, nil
	}
	if remaining, ok := a.r.(interface{ Len() int }); ok && length > int64(remaining.Len()) {
		return nil, io.ErrUnexpectedEOF
	}
	buf := make([]byte, length)
	_, err = io.ReadFull(a.r, buf)
	if err != nil {
		return nil, fmt.Errorf("readBytes data: %w", err)
	}
	return buf, nil
}

func (a *avroReader) readArrayBlockCount() (int64, error) {
	count, err := a.readLong()
	if err != nil {
		return 0, err
	}
	if count < 0 {
		count = -count
		if _, err := a.readLong(); err != nil {
			return 0, fmt.Errorf("readArrayBlockCount byte-size: %w", err)
		}
	}
	return count, nil
}

func readAvroArray[T any](a *avroReader, readElem func() (T, error)) ([]T, error) {
	var result []T
	for {
		count, err := a.readArrayBlockCount()
		if err != nil {
			return nil, err
		}
		if count == 0 {
			break
		}
		for i := int64(0); i < count; i++ {
			elem, err := readElem()
			if err != nil {
				return nil, err
			}
			result = append(result, elem)
		}
	}
	return result, nil
}

func readAvroMap[V any](a *avroReader, readValue func() (V, error)) (map[string]V, error) {
	result := make(map[string]V)
	for {
		count, err := a.readArrayBlockCount()
		if err != nil {
			return nil, err
		}
		if count == 0 {
			break
		}
		for i := int64(0); i < count; i++ {
			key, err := a.readString()
			if err != nil {
				return nil, fmt.Errorf("map key: %w", err)
			}
			val, err := readValue()
			if err != nil {
				return nil, fmt.Errorf("map value for key %q: %w", key, err)
			}
			result[key] = val
		}
	}
	return result, nil
}

func (a *avroReader) readColumnGroupFile(hasProperties bool) (ColumnGroupFile, error) {
	var f ColumnGroupFile
	var err error
	if f.Path, err = a.readString(); err != nil {
		return f, fmt.Errorf("ColumnGroupFile.path: %w", err)
	}
	if f.StartIndex, err = a.readLong(); err != nil {
		return f, fmt.Errorf("ColumnGroupFile.start_index: %w", err)
	}
	if f.EndIndex, err = a.readLong(); err != nil {
		return f, fmt.Errorf("ColumnGroupFile.end_index: %w", err)
	}
	if hasProperties {
		f.Properties, err = readAvroMap(a, func() (string, error) {
			return a.readString()
		})
		if err != nil {
			return f, fmt.Errorf("ColumnGroupFile.properties: %w", err)
		}
	} else {
		if f.Metadata, err = a.readBytes(); err != nil {
			return f, fmt.Errorf("ColumnGroupFile.metadata: %w", err)
		}
	}
	return f, nil
}

func (a *avroReader) readColumnGroup(hasFileProperties bool) (ColumnGroup, error) {
	var cg ColumnGroup
	var err error

	cg.Columns, err = readAvroArray(a, func() (string, error) {
		return a.readString()
	})
	if err != nil {
		return cg, fmt.Errorf("ColumnGroup.columns: %w", err)
	}

	cg.Files, err = readAvroArray(a, func() (ColumnGroupFile, error) {
		return a.readColumnGroupFile(hasFileProperties)
	})
	if err != nil {
		return cg, fmt.Errorf("ColumnGroup.files: %w", err)
	}

	cg.Format, err = a.readString()
	if err != nil {
		return cg, fmt.Errorf("ColumnGroup.format: %w", err)
	}
	return cg, nil
}

func (a *avroReader) readDeltaLog() (DeltaLog, error) {
	var dl DeltaLog
	var err error
	if dl.Path, err = a.readString(); err != nil {
		return dl, fmt.Errorf("DeltaLog.path: %w", err)
	}
	typeInt, err := a.readInt()
	if err != nil {
		return dl, fmt.Errorf("DeltaLog.type: %w", err)
	}
	dl.Type = DeltaLogType(typeInt)
	if dl.NumEntries, err = a.readLong(); err != nil {
		return dl, fmt.Errorf("DeltaLog.num_entries: %w", err)
	}
	return dl, nil
}

// readIndex decodes a single Index. Version 6 added typed artifact metadata
// between column_name and properties; earlier versions use the original four fields.
func (a *avroReader) readIndex(hasTypedMetadata bool) (Index, error) {
	var idx Index
	var err error
	if idx.ColumnName, err = a.readString(); err != nil {
		return idx, fmt.Errorf("index.column_name: %w", err)
	}
	if hasTypedMetadata {
		if idx.IndexName, err = a.readString(); err != nil {
			return idx, fmt.Errorf("index.index_name: %w", err)
		}
	}
	if idx.IndexType, err = a.readString(); err != nil {
		return idx, fmt.Errorf("index.index_type: %w", err)
	}
	if idx.Path, err = a.readString(); err != nil {
		return idx, fmt.Errorf("index.path: %w", err)
	}
	if hasTypedMetadata {
		if idx.FieldID, err = a.readLong(); err != nil {
			return idx, fmt.Errorf("index.field_id: %w", err)
		}
		if idx.IndexID, err = a.readLong(); err != nil {
			return idx, fmt.Errorf("index.index_id: %w", err)
		}
		if idx.BuildID, err = a.readLong(); err != nil {
			return idx, fmt.Errorf("index.build_id: %w", err)
		}
		if idx.IndexVersion, err = a.readLong(); err != nil {
			return idx, fmt.Errorf("index.index_version: %w", err)
		}
		if idx.NumRows, err = a.readLong(); err != nil {
			return idx, fmt.Errorf("index.num_rows: %w", err)
		}
		if idx.SerializedSize, err = a.readLong(); err != nil {
			return idx, fmt.Errorf("index.serialized_size: %w", err)
		}
		if idx.MemSize, err = a.readLong(); err != nil {
			return idx, fmt.Errorf("index.mem_size: %w", err)
		}
		if idx.CurrentIndexVersion, err = a.readInt(); err != nil {
			return idx, fmt.Errorf("index.current_index_version: %w", err)
		}
		if idx.CurrentScalarIndexVersion, err = a.readInt(); err != nil {
			return idx, fmt.Errorf("index.current_scalar_index_version: %w", err)
		}
		if idx.IndexStorePathVersion, err = a.readInt(); err != nil {
			return idx, fmt.Errorf("index.index_store_path_version: %w", err)
		}
		idx.IndexFileKeys, err = readAvroArray(a, func() (string, error) {
			return a.readString()
		})
		if err != nil {
			return idx, fmt.Errorf("index.index_file_keys: %w", err)
		}
	}
	idx.Properties, err = readAvroMap(a, func() (string, error) {
		return a.readString()
	})
	if err != nil {
		return idx, fmt.Errorf("index.properties: %w", err)
	}
	return idx, nil
}

func (a *avroReader) readLobFileInfo() (LobFileInfo, error) {
	var lob LobFileInfo
	var err error
	if lob.Path, err = a.readString(); err != nil {
		return lob, fmt.Errorf("lob_file.path: %w", err)
	}
	if lob.FieldID, err = a.readLong(); err != nil {
		return lob, fmt.Errorf("lob_file.field_id: %w", err)
	}
	if lob.TotalRows, err = a.readLong(); err != nil {
		return lob, fmt.Errorf("lob_file.total_rows: %w", err)
	}
	if lob.ValidRows, err = a.readLong(); err != nil {
		return lob, fmt.Errorf("lob_file.valid_rows: %w", err)
	}
	if lob.FileSizeBytes, err = a.readLong(); err != nil {
		return lob, fmt.Errorf("lob_file.file_size_bytes: %w", err)
	}
	return lob, nil
}

// readStatistics decodes a single Statistics (v3 format).
// Encoding order: paths(array<string>), metadata(map<string,string>)
func (a *avroReader) readStatistics() (Statistics, error) {
	var stat Statistics
	var err error
	stat.Paths, err = readAvroArray(a, func() (string, error) {
		return a.readString()
	})
	if err != nil {
		return stat, fmt.Errorf("statistics.paths: %w", err)
	}
	stat.Metadata, err = readAvroMap(a, func() (string, error) {
		return a.readString()
	})
	if err != nil {
		return stat, fmt.Errorf("statistics.metadata: %w", err)
	}
	return stat, nil
}

// parseAvroOCF validates the container framing before decoding its single
// snapshot with the embedded writer schema. Validating framing explicitly also
// prevents a truncated sync marker from being treated as a clean end of file.
func parseAvroOCF(r *bytes.Reader) (*Manifest, error) {
	ar := &avroReader{r: r}
	meta, err := readAvroMap(ar, func() ([]byte, error) { return ar.readBytes() })
	if err != nil {
		return nil, fmt.Errorf("read OCF metadata: %w", err)
	}
	schema, err := avro.ParseBytes(meta["avro.schema"])
	if err != nil {
		return nil, fmt.Errorf("read OCF schema: %w", err)
	}
	var codec ocf.Codec
	switch string(meta["avro.codec"]) {
	case "", "null":
		codec = &ocf.NullCodec{}
	case "deflate":
		codec = &ocf.DeflateCodec{}
	case "snappy":
		codec = &ocf.SnappyCodec{}
	default:
		return nil, fmt.Errorf("unsupported OCF codec %q", meta["avro.codec"])
	}
	var syncMarker, blockSync [16]byte
	if _, err := io.ReadFull(r, syncMarker[:]); err != nil {
		return nil, fmt.Errorf("read OCF sync: %w", err)
	}
	count, err := ar.readLong()
	if err != nil {
		return nil, fmt.Errorf("read OCF record count: %w", err)
	}
	if count != 1 {
		return nil, fmt.Errorf("OCF must contain one manifest record, got %d", count)
	}
	size, err := ar.readLong()
	if err != nil {
		return nil, fmt.Errorf("read OCF block size: %w", err)
	}
	if size < 0 || int64(r.Len()) < size || int64(r.Len())-size < 16 {
		return nil, fmt.Errorf("invalid or truncated OCF block size %d", size)
	}
	compressed := make([]byte, size)
	if _, err := io.ReadFull(r, compressed); err != nil {
		return nil, err
	}
	if _, err := io.ReadFull(r, blockSync[:]); err != nil {
		return nil, err
	}
	if blockSync != syncMarker {
		return nil, fmt.Errorf("OCF sync marker mismatch")
	}
	if r.Len() != 0 {
		return nil, fmt.Errorf("unexpected trailing data after OCF manifest record")
	}
	block, err := codec.Decode(compressed)
	if err != nil {
		return nil, fmt.Errorf("decompress OCF manifest: %w", err)
	}
	m := &Manifest{}
	if err := avro.Unmarshal(schema, block, m); err != nil {
		return nil, fmt.Errorf("decode manifest record: %w", err)
	}
	m.Format = "avro_ocf"
	m.Version = detectOCFManifestVersion(meta["avro.schema"])
	return m, nil
}

// detectOCFManifestVersion infers the writer version from fields added by each OCF schema revision.
func detectOCFManifestVersion(schemaJSON []byte) int32 {
	var schema any
	if err := json.Unmarshal(schemaJSON, &schema); err != nil {
		return VersionV3
	}
	switch {
	case avroRecordHasField(schema, "Index", "index_name"):
		return VersionV6
	case avroRecordHasField(schema, "Manifest", "lob_files"):
		return VersionV5
	case avroRecordHasField(schema, "ColumnGroupFile", "properties"):
		return VersionV4
	default:
		return VersionV3
	}
}

func avroRecordHasField(node any, recordName, fieldName string) bool {
	switch value := node.(type) {
	case map[string]any:
		if name, _ := value["name"].(string); name == recordName {
			if fields, ok := value["fields"].([]any); ok {
				for _, field := range fields {
					fieldMap, ok := field.(map[string]any)
					if !ok {
						continue
					}
					if name, _ := fieldMap["name"].(string); name == fieldName {
						return true
					}
				}
			}
		}
		for _, child := range value {
			if avroRecordHasField(child, recordName, fieldName) {
				return true
			}
		}
	case []any:
		for _, child := range value {
			if avroRecordHasField(child, recordName, fieldName) {
				return true
			}
		}
	}
	return false
}

// parseLegacyManifest parses the legacy MILV format (raw Avro binary with magic + version prefix).
func parseLegacyManifest(r io.Reader) (*Manifest, error) {
	ar := &avroReader{r: r}
	m := &Manifest{Format: "legacy_milv"}
	var err error

	// 1. Magic
	m.Magic, err = ar.readInt()
	if err != nil {
		return nil, fmt.Errorf("reading magic: %w", err)
	}
	var magicBytes [4]byte
	binary.BigEndian.PutUint32(magicBytes[:], uint32(m.Magic))
	m.MagicStr = string(magicBytes[:])

	if m.Magic != Magic {
		return nil, fmt.Errorf("invalid magic number: expected 0x%08X (%q), got 0x%08X (%q)",
			Magic, "MILV", m.Magic, m.MagicStr)
	}

	// 2. Version
	m.Version, err = ar.readInt()
	if err != nil {
		return nil, fmt.Errorf("reading version: %w", err)
	}
	if m.Version < 1 || (m.Version > VersionV3 && m.Version != VersionV6) {
		return nil, fmt.Errorf("unsupported manifest version: %d (expected 1-3 or %d)", m.Version, VersionV6)
	}

	// 3. Column groups: array<ColumnGroup>
	// Supported raw MILV versions keep the legacy metadata (bytes) layout.
	m.ColumnGroups, err = readAvroArray(ar, func() (ColumnGroup, error) {
		return ar.readColumnGroup(false)
	})
	if err != nil {
		return nil, fmt.Errorf("reading column_groups: %w", err)
	}

	// 4. Delta logs: array<DeltaLog>
	m.DeltaLogs, err = readAvroArray(ar, func() (DeltaLog, error) {
		return ar.readDeltaLog()
	})
	if err != nil {
		return nil, fmt.Errorf("reading delta_logs: %w", err)
	}

	// 5. Stats: version-dependent format
	if m.Version >= VersionV3 {
		// v3: map<string, Statistics> where Statistics = {paths: array<string>, metadata: map<string,string>}
		m.Stats, err = readAvroMap(ar, func() (*Statistics, error) {
			stat, err := ar.readStatistics()
			if err != nil {
				return nil, err
			}
			return &stat, nil
		})
		if err != nil {
			return nil, fmt.Errorf("reading stats (v3): %w", err)
		}
	} else {
		// v1/v2: map<string, array<string>> — convert to Statistics with empty metadata
		legacyStats, err := readAvroMap(ar, func() ([]string, error) {
			return readAvroArray(ar, func() (string, error) {
				return ar.readString()
			})
		})
		if err != nil {
			return nil, fmt.Errorf("reading stats (legacy): %w", err)
		}
		m.Stats = make(map[string]*Statistics, len(legacyStats))
		for k, paths := range legacyStats {
			m.Stats[k] = &Statistics{Paths: paths}
		}
	}

	// 6. Indexes: v2-v3 use the original layout; v6 uses typed metadata.
	if m.Version >= 2 {
		m.Indexes, err = readAvroArray(ar, func() (Index, error) {
			return ar.readIndex(m.Version == VersionV6)
		})
		if err != nil {
			return nil, fmt.Errorf("reading indexes: %w", err)
		}
	}

	// 7. LOB files: array<LobFileInfo> (v6)
	if m.Version == VersionV6 {
		m.LobFiles, err = readAvroArray(ar, func() (LobFileInfo, error) {
			return ar.readLobFileInfo()
		})
		if err != nil {
			return nil, fmt.Errorf("reading lob_files: %w", err)
		}
	}

	return m, nil
}

// Parse detects the container format and reads one immutable manifest snapshot.
func Parse(r io.ReadSeeker) (*Manifest, error) {
	if _, err := r.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	if len(data) < 4 {
		return nil, io.ErrUnexpectedEOF
	}
	if bytes.Equal(data[:4], avroOCFMagic) {
		return parseAvroOCF(bytes.NewReader(data[4:]))
	}
	return parseLegacyManifest(bytes.NewReader(data))
}
