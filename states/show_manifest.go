package states

import (
	"bytes"
	"compress/flate"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path"
	"strings"

	"github.com/minio/minio-go/v7"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/oss"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
)

type ShowManifestParam struct {
	framework.ParamBase `use:"show manifest" desc:"parse and display manifest file from S3 for segments"`
	CollectionID        int64  `name:"collection" default:"0" desc:"collection id to filter with"`
	SegmentID           int64  `name:"segment" default:"0" desc:"segment id to display"`
	MinioAddress        string `name:"minioAddr" desc:"override minio address"`
	SkipBucketCheck     bool   `name:"skipBucketCheck" default:"false" desc:"skip bucket exist check"`
	JSONOutput          bool   `name:"json" default:"false" desc:"output as JSON"`
}

func (s *InstanceState) ShowManifestCommand(ctx context.Context, p *ShowManifestParam) error {
	if p.CollectionID == 0 && p.SegmentID == 0 {
		return fmt.Errorf("at least one of --collection or --segment must be specified")
	}

	segments, err := common.ListSegments(ctx, s.client, s.basePath, func(seg *models.Segment) bool {
		return (p.CollectionID == 0 || seg.CollectionID == p.CollectionID) &&
			(p.SegmentID == 0 || seg.ID == p.SegmentID) &&
			seg.State != commonpb.SegmentState_Dropped
	})
	if err != nil {
		return err
	}

	// Filter segments that have manifest paths
	var manifestSegments []*models.Segment
	for _, seg := range segments {
		if seg.GetManifestPath() != "" {
			manifestSegments = append(manifestSegments, seg)
		}
	}

	if len(manifestSegments) == 0 {
		fmt.Println("No segments with manifest path found")
		return nil
	}

	params := []oss.MinioConnectParam{oss.WithSkipCheckBucket(p.SkipBucketCheck)}
	if p.MinioAddress != "" {
		params = append(params, oss.WithMinioAddr(p.MinioAddress))
	}

	minioClient, bucketName, rootPath, err := s.GetMinioClientFromCfg(ctx, params...)
	if err != nil {
		return fmt.Errorf("failed to create minio client: %w", err)
	}

	for _, seg := range manifestSegments {
		rawManifest := seg.GetManifestPath()
		fmt.Printf("=== Segment %d (Collection: %d, Partition: %d) ===\n", seg.ID, seg.CollectionID, seg.PartitionID)
		fmt.Printf("Manifest Raw: %s\n", rawManifest)

		// ManifestPath is JSON: {"ver":2,"base_path":"files/insert_log/..."}
		// Actual file: {rootPath}/{base_path}/_metadata/manifest-{ver}.arvo
		var manifestRef struct {
			Ver      int    `json:"ver"`
			BasePath string `json:"base_path"`
		}
		if err := json.Unmarshal([]byte(rawManifest), &manifestRef); err != nil {
			fmt.Printf("Error parsing manifest path JSON: %v\n\n", err)
			continue
		}

		basePath := strings.ReplaceAll(manifestRef.BasePath, "ROOT_PATH", rootPath)
		manifestPath := path.Join(basePath, "_metadata", fmt.Sprintf("manifest-%d.avro", manifestRef.Ver))
		fmt.Printf("Manifest File: %s\n", manifestPath)

		obj, err := minioClient.GetObject(ctx, bucketName, manifestPath, minio.GetObjectOptions{})
		if err != nil {
			fmt.Printf("Error getting object: %v\n\n", err)
			continue
		}

		manifest, err := parseManifest(obj)
		obj.Close()
		if err != nil {
			fmt.Printf("Error parsing manifest: %v\n\n", err)
			continue
		}

		if p.JSONOutput {
			enc := json.NewEncoder(os.Stdout)
			enc.SetIndent("", "  ")
			if err := enc.Encode(manifest); err != nil {
				fmt.Printf("Error encoding JSON: %v\n", err)
			}
		} else {
			printManifest(manifest)
		}
		fmt.Println()
	}

	return nil
}

// Manifest parsing types and logic (Avro binary encoding)

const manifestMagic int32 = 0x4D494C56 // "MILV"

// Manifest format evolution:
//   - v1: initial (column_groups, delta_logs, stats as map<string, array<string>>)
//   - v2: added indexes
//   - v3: stats changed to map<string, Statistics>
//   - v4: ColumnGroupFile.metadata (bytes) replaced by properties (map<string,string>)
const manifestVersionV4 = 4

// avroOCFMagic is the 4-byte magic header for Avro Object Container Format files.
var avroOCFMagic = []byte{'O', 'b', 'j', 0x01}

type manifestColumnGroupFile struct {
	Path       string            `json:"path"`
	StartIndex int64             `json:"start_index"`
	EndIndex   int64             `json:"end_index"`
	Metadata   []byte            `json:"metadata,omitempty"`
	Properties map[string]string `json:"properties,omitempty"`
}

type manifestColumnGroup struct {
	Columns []string                  `json:"columns"`
	Files   []manifestColumnGroupFile `json:"files"`
	Format  string                    `json:"format"`
}

type manifestDeltaLogType int32

const (
	manifestDeltaLogTypePrimaryKey manifestDeltaLogType = 0
	manifestDeltaLogTypePositional manifestDeltaLogType = 1
	manifestDeltaLogTypeEquality   manifestDeltaLogType = 2
)

func (t manifestDeltaLogType) String() string {
	switch t {
	case manifestDeltaLogTypePrimaryKey:
		return "PRIMARY_KEY"
	case manifestDeltaLogTypePositional:
		return "POSITIONAL"
	case manifestDeltaLogTypeEquality:
		return "EQUALITY"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", int32(t))
	}
}

func (t manifestDeltaLogType) MarshalJSON() ([]byte, error) {
	return json.Marshal(t.String())
}

type manifestDeltaLog struct {
	Path       string               `json:"path"`
	Type       manifestDeltaLogType `json:"type"`
	NumEntries int64                `json:"num_entries"`
}

// manifestIndex represents index metadata for a column.
type manifestIndex struct {
	ColumnName string            `json:"column_name"`
	IndexType  string            `json:"index_type"`
	Path       string            `json:"path"`
	Properties map[string]string `json:"properties,omitempty"`
}

// manifestStatistics represents a stats entry with file paths and optional metadata.
type manifestStatistics struct {
	Paths    []string          `json:"paths"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

type manifest struct {
	Format       string                         `json:"format"`
	Magic        int32                          `json:"magic,omitempty"`
	MagicStr     string                         `json:"magic_str,omitempty"`
	Version      int32                          `json:"version"`
	ColumnGroups []manifestColumnGroup          `json:"column_groups"`
	DeltaLogs    []manifestDeltaLog             `json:"delta_logs"`
	Stats        map[string]*manifestStatistics `json:"stats"`
	Indexes      []manifestIndex                `json:"indexes,omitempty"`
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
			return 0, fmt.Errorf("readLong: %w", err)
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

func (a *avroReader) readColumnGroupFile(version int32) (manifestColumnGroupFile, error) {
	var f manifestColumnGroupFile
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
	if version >= manifestVersionV4 {
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

func (a *avroReader) readColumnGroup(version int32) (manifestColumnGroup, error) {
	var cg manifestColumnGroup
	var err error

	cg.Columns, err = readAvroArray(a, func() (string, error) {
		return a.readString()
	})
	if err != nil {
		return cg, fmt.Errorf("ColumnGroup.columns: %w", err)
	}

	cg.Files, err = readAvroArray(a, func() (manifestColumnGroupFile, error) {
		return a.readColumnGroupFile(version)
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

func (a *avroReader) readDeltaLog() (manifestDeltaLog, error) {
	var dl manifestDeltaLog
	var err error
	if dl.Path, err = a.readString(); err != nil {
		return dl, fmt.Errorf("DeltaLog.path: %w", err)
	}
	typeInt, err := a.readInt()
	if err != nil {
		return dl, fmt.Errorf("DeltaLog.type: %w", err)
	}
	dl.Type = manifestDeltaLogType(typeInt)
	if dl.NumEntries, err = a.readLong(); err != nil {
		return dl, fmt.Errorf("DeltaLog.num_entries: %w", err)
	}
	return dl, nil
}

// readIndex decodes a single Index.
// Encoding order: column_name(string), index_type(string), path(string), properties(map<string,string>)
func (a *avroReader) readIndex() (manifestIndex, error) {
	var idx manifestIndex
	var err error
	if idx.ColumnName, err = a.readString(); err != nil {
		return idx, fmt.Errorf("index.column_name: %w", err)
	}
	if idx.IndexType, err = a.readString(); err != nil {
		return idx, fmt.Errorf("index.index_type: %w", err)
	}
	if idx.Path, err = a.readString(); err != nil {
		return idx, fmt.Errorf("index.path: %w", err)
	}
	idx.Properties, err = readAvroMap(a, func() (string, error) {
		return a.readString()
	})
	if err != nil {
		return idx, fmt.Errorf("index.properties: %w", err)
	}
	return idx, nil
}

// readStatistics decodes a single Statistics (v3 format).
// Encoding order: paths(array<string>), metadata(map<string,string>)
func (a *avroReader) readStatistics() (manifestStatistics, error) {
	var stat manifestStatistics
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

// readManifestRecord decodes the Manifest record fields from Avro binary encoding.
// Field order: column_groups, delta_logs, stats(map<string, Statistics>), indexes
func readManifestRecord(ar *avroReader, version int32) (*manifest, error) {
	m := &manifest{}
	var err error

	// 1. Column groups: array<ColumnGroup>
	m.ColumnGroups, err = readAvroArray(ar, func() (manifestColumnGroup, error) {
		return ar.readColumnGroup(version)
	})
	if err != nil {
		return nil, fmt.Errorf("reading column_groups: %w", err)
	}

	// 2. Delta logs: array<DeltaLog>
	m.DeltaLogs, err = readAvroArray(ar, func() (manifestDeltaLog, error) {
		return ar.readDeltaLog()
	})
	if err != nil {
		return nil, fmt.Errorf("reading delta_logs: %w", err)
	}

	// 3. Stats: map<string, Statistics>
	m.Stats, err = readAvroMap(ar, func() (*manifestStatistics, error) {
		stat, err := ar.readStatistics()
		if err != nil {
			return nil, err
		}
		return &stat, nil
	})
	if err != nil {
		return nil, fmt.Errorf("reading stats: %w", err)
	}

	// 4. Indexes: array<Index>
	m.Indexes, err = readAvroArray(ar, func() (manifestIndex, error) {
		return ar.readIndex()
	})
	if err != nil {
		return nil, fmt.Errorf("reading indexes: %w", err)
	}

	return m, nil
}

// parseAvroOCF parses an Avro Object Container Format file.
// The reader should be positioned right after the 4-byte "Obj\x01" magic.
func parseAvroOCF(r io.Reader) (*manifest, error) {
	ar := &avroReader{r: r}

	// Read file metadata: map<string, bytes>
	meta, err := readAvroMap(ar, func() ([]byte, error) {
		return ar.readBytes()
	})
	if err != nil {
		return nil, fmt.Errorf("reading OCF metadata: %w", err)
	}

	// Extract codec (default "null")
	codec := "null"
	if codecBytes, ok := meta["avro.codec"]; ok {
		codec = string(codecBytes)
	}

	// Detect manifest version from the embedded Avro schema.
	// OCF files embed the writer's schema; the shape of ColumnGroupFile tells us
	// whether this is v4 (properties: map<string,string>) or v3 (metadata: bytes).
	version := int32(3)
	if schemaBytes, ok := meta["avro.schema"]; ok {
		version = detectOCFManifestVersion(schemaBytes)
	}

	// Read 16-byte sync marker
	var syncMarker [16]byte
	if _, err := io.ReadFull(r, syncMarker[:]); err != nil {
		return nil, fmt.Errorf("reading sync marker: %w", err)
	}

	// Read data blocks until EOF
	var allData []byte
	for {
		// Read object count (long)
		objectCount, err := ar.readLong()
		if err != nil {
			// EOF means no more blocks
			break
		}
		if objectCount <= 0 {
			break
		}

		// Read block byte size (long)
		blockSize, err := ar.readLong()
		if err != nil {
			return nil, fmt.Errorf("reading block size: %w", err)
		}

		// Read block data
		blockData := make([]byte, blockSize)
		if _, err := io.ReadFull(r, blockData); err != nil {
			return nil, fmt.Errorf("reading block data: %w", err)
		}

		// Decompress if needed
		switch codec {
		case "null":
			allData = append(allData, blockData...)
		case "deflate":
			fr := flate.NewReader(bytes.NewReader(blockData))
			decompressed, err := io.ReadAll(fr)
			fr.Close()
			if err != nil {
				return nil, fmt.Errorf("deflate decompression: %w", err)
			}
			allData = append(allData, decompressed...)
		case "snappy":
			decompressed, err := manifestSnappyDecode(blockData)
			if err != nil {
				return nil, fmt.Errorf("snappy decompression: %w", err)
			}
			allData = append(allData, decompressed...)
		default:
			return nil, fmt.Errorf("unsupported codec: %s", codec)
		}

		// Read and verify sync marker
		var blockSync [16]byte
		if _, err := io.ReadFull(r, blockSync[:]); err != nil {
			return nil, fmt.Errorf("reading block sync marker: %w", err)
		}
		if blockSync != syncMarker {
			return nil, fmt.Errorf("sync marker mismatch")
		}
	}

	if len(allData) == 0 {
		return nil, fmt.Errorf("no data blocks found in OCF file")
	}

	// Decode the manifest record from the accumulated block data
	blockReader := &avroReader{r: bytes.NewReader(allData)}
	m, err := readManifestRecord(blockReader, version)
	if err != nil {
		return nil, fmt.Errorf("decoding manifest record: %w", err)
	}

	m.Format = "avro_ocf"
	m.Version = version

	return m, nil
}

// detectOCFManifestVersion inspects the Avro schema JSON embedded in an OCF file's
// metadata and returns the manifest version inferred from the ColumnGroupFile record.
//   - "properties" field present  → v4
//   - "metadata" field present    → v3 (or earlier OCF writer)
func detectOCFManifestVersion(schemaJSON []byte) int32 {
	var schema any
	if err := json.Unmarshal(schemaJSON, &schema); err != nil {
		return 3
	}
	if columnGroupFileHasField(schema, "properties") {
		return 4
	}
	return 3
}

// columnGroupFileHasField walks a decoded Avro schema tree and reports whether the
// record named "ColumnGroupFile" has a field with the given name.
func columnGroupFileHasField(node any, fieldName string) bool {
	switch v := node.(type) {
	case map[string]any:
		if name, _ := v["name"].(string); name == "ColumnGroupFile" {
			if fields, ok := v["fields"].([]any); ok {
				for _, f := range fields {
					fm, ok := f.(map[string]any)
					if !ok {
						continue
					}
					if fn, _ := fm["name"].(string); fn == fieldName {
						return true
					}
				}
			}
		}
		for _, child := range v {
			if columnGroupFileHasField(child, fieldName) {
				return true
			}
		}
	case []any:
		for _, child := range v {
			if columnGroupFileHasField(child, fieldName) {
				return true
			}
		}
	}
	return false
}

// manifestSnappyDecode decodes Avro-framed snappy data.
// Avro uses snappy block format: compressed data followed by a 4-byte CRC32C checksum of the uncompressed data.
func manifestSnappyDecode(data []byte) ([]byte, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("snappy block too short: %d bytes", len(data))
	}

	// The last 4 bytes are a CRC32C checksum of the uncompressed data
	compressedData := data[:len(data)-4]
	expectedCRC := binary.BigEndian.Uint32(data[len(data)-4:])

	decoded, err := manifestSnappyDecodeBlock(compressedData)
	if err != nil {
		return nil, err
	}

	// Verify CRC32C checksum
	actualCRC := crc32.Checksum(decoded, crc32.MakeTable(crc32.Castagnoli))
	if actualCRC != expectedCRC {
		return nil, fmt.Errorf("snappy CRC32C mismatch: expected 0x%08x, got 0x%08x", expectedCRC, actualCRC)
	}

	return decoded, nil
}

// manifestSnappyDecodeBlock decodes a raw snappy-compressed block.
func manifestSnappyDecodeBlock(src []byte) ([]byte, error) {
	if len(src) == 0 {
		return nil, nil
	}

	// Read preamble: uncompressed length as varint
	dLen, n := manifestDecodeVarint(src)
	if n <= 0 {
		return nil, fmt.Errorf("snappy: invalid varint in preamble")
	}
	src = src[n:]

	dst := make([]byte, 0, dLen)

	for len(src) > 0 {
		tag := src[0]
		tagType := tag & 0x03

		switch tagType {
		case 0: // Literal
			litLen := int(tag>>2) + 1
			src = src[1:]
			switch litLen {
			case 60 + 1:
				if len(src) < 1 {
					return nil, fmt.Errorf("snappy: truncated literal length")
				}
				litLen = int(src[0]) + 1
				src = src[1:]
			case 61 + 1:
				if len(src) < 2 {
					return nil, fmt.Errorf("snappy: truncated literal length")
				}
				litLen = int(src[0]) | int(src[1])<<8 + 1
				src = src[2:]
			case 62 + 1:
				if len(src) < 3 {
					return nil, fmt.Errorf("snappy: truncated literal length")
				}
				litLen = int(src[0]) | int(src[1])<<8 | int(src[2])<<16 + 1
				src = src[3:]
			case 63 + 1:
				if len(src) < 4 {
					return nil, fmt.Errorf("snappy: truncated literal length")
				}
				litLen = int(src[0]) | int(src[1])<<8 | int(src[2])<<16 | int(src[3])<<24 + 1
				src = src[4:]
			}
			if len(src) < litLen {
				return nil, fmt.Errorf("snappy: truncated literal data")
			}
			dst = append(dst, src[:litLen]...)
			src = src[litLen:]

		case 1: // Copy with 1-byte offset
			length := int(tag>>2)&0x07 + 4
			if len(src) < 2 {
				return nil, fmt.Errorf("snappy: truncated copy1")
			}
			offset := int(tag)&0xe0<<3 | int(src[1])
			src = src[2:]
			if offset == 0 || offset > len(dst) {
				return nil, fmt.Errorf("snappy: invalid copy1 offset %d (output length %d)", offset, len(dst))
			}
			for i := 0; i < length; i++ {
				dst = append(dst, dst[len(dst)-offset])
			}

		case 2: // Copy with 2-byte offset
			length := int(tag>>2) + 1
			if len(src) < 3 {
				return nil, fmt.Errorf("snappy: truncated copy2")
			}
			offset := int(src[1]) | int(src[2])<<8
			src = src[3:]
			if offset == 0 || offset > len(dst) {
				return nil, fmt.Errorf("snappy: invalid copy2 offset %d (output length %d)", offset, len(dst))
			}
			for i := 0; i < length; i++ {
				dst = append(dst, dst[len(dst)-offset])
			}

		case 3: // Copy with 4-byte offset
			length := int(tag>>2) + 1
			if len(src) < 5 {
				return nil, fmt.Errorf("snappy: truncated copy4")
			}
			offset := int(src[1]) | int(src[2])<<8 | int(src[3])<<16 | int(src[4])<<24
			src = src[5:]
			if offset == 0 || offset > len(dst) {
				return nil, fmt.Errorf("snappy: invalid copy4 offset %d (output length %d)", offset, len(dst))
			}
			for i := 0; i < length; i++ {
				dst = append(dst, dst[len(dst)-offset])
			}
		}
	}

	if uint64(len(dst)) != dLen {
		return nil, fmt.Errorf("snappy: output length mismatch: got %d, expected %d", len(dst), dLen)
	}
	return dst, nil
}

// manifestDecodeVarint decodes a little-endian base-128 varint.
func manifestDecodeVarint(buf []byte) (uint64, int) {
	var val uint64
	var shift uint
	for i, b := range buf {
		val |= uint64(b&0x7f) << shift
		if b&0x80 == 0 {
			return val, i + 1
		}
		shift += 7
		if shift >= 64 {
			return 0, -1
		}
	}
	return 0, -1
}

// parseLegacyManifest parses the legacy MILV format (raw Avro binary with magic + version prefix).
func parseLegacyManifest(r io.Reader) (*manifest, error) {
	ar := &avroReader{r: r}
	m := &manifest{Format: "legacy_milv"}
	var err error

	// 1. Magic
	m.Magic, err = ar.readInt()
	if err != nil {
		return nil, fmt.Errorf("reading magic: %w", err)
	}
	var magicBytes [4]byte
	binary.BigEndian.PutUint32(magicBytes[:], uint32(m.Magic))
	m.MagicStr = string(magicBytes[:])

	if m.Magic != manifestMagic {
		return nil, fmt.Errorf("invalid magic number: expected 0x%08X (%q), got 0x%08X (%q)",
			manifestMagic, "MILV", m.Magic, m.MagicStr)
	}

	// 2. Version
	m.Version, err = ar.readInt()
	if err != nil {
		return nil, fmt.Errorf("reading version: %w", err)
	}
	if m.Version < 1 || m.Version > 3 {
		return nil, fmt.Errorf("unsupported manifest version: %d (expected 1-3)", m.Version)
	}

	// 3. Column groups: array<ColumnGroup>
	// Legacy MILV format always used metadata (bytes); it was replaced by OCF before v4.
	m.ColumnGroups, err = readAvroArray(ar, func() (manifestColumnGroup, error) {
		return ar.readColumnGroup(m.Version)
	})
	if err != nil {
		return nil, fmt.Errorf("reading column_groups: %w", err)
	}

	// 4. Delta logs: array<DeltaLog>
	m.DeltaLogs, err = readAvroArray(ar, func() (manifestDeltaLog, error) {
		return ar.readDeltaLog()
	})
	if err != nil {
		return nil, fmt.Errorf("reading delta_logs: %w", err)
	}

	// 5. Stats: version-dependent format
	if m.Version >= 3 {
		// v3: map<string, Statistics> where Statistics = {paths: array<string>, metadata: map<string,string>}
		m.Stats, err = readAvroMap(ar, func() (*manifestStatistics, error) {
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
		// v1/v2: map<string, array<string>> — convert to manifestStatistics with empty metadata
		legacyStats, err := readAvroMap(ar, func() ([]string, error) {
			return readAvroArray(ar, func() (string, error) {
				return ar.readString()
			})
		})
		if err != nil {
			return nil, fmt.Errorf("reading stats (legacy): %w", err)
		}
		m.Stats = make(map[string]*manifestStatistics, len(legacyStats))
		for k, paths := range legacyStats {
			m.Stats[k] = &manifestStatistics{Paths: paths}
		}
	}

	// 6. Indexes: array<Index> (v2+ only)
	if m.Version >= 2 {
		m.Indexes, err = readAvroArray(ar, func() (manifestIndex, error) {
			return ar.readIndex()
		})
		if err != nil {
			return nil, fmt.Errorf("reading indexes: %w", err)
		}
	}

	return m, nil
}

// parseManifest detects the format and parses accordingly.
func parseManifest(r io.ReadSeeker) (*manifest, error) {
	// Read first 4 bytes to detect format
	var header [4]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return nil, fmt.Errorf("reading header: %w", err)
	}

	// Seek back to start
	if _, err := r.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("seeking to start: %w", err)
	}

	if bytes.Equal(header[:], avroOCFMagic) {
		// Avro Object Container Format — skip the 4-byte magic
		if _, err := r.Seek(4, io.SeekStart); err != nil {
			return nil, fmt.Errorf("seeking past OCF magic: %w", err)
		}
		return parseAvroOCF(r)
	}

	// Legacy MILV format
	return parseLegacyManifest(r)
}

func printManifest(m *manifest) {
	fmt.Printf("Format:  %s\n", m.Format)
	if m.Format == "legacy_milv" {
		fmt.Printf("Magic:   0x%08X (%q)\n", m.Magic, m.MagicStr)
	}
	fmt.Printf("Version: %d\n", m.Version)

	fmt.Printf("\nColumn Groups (%d):\n", len(m.ColumnGroups))
	for i, cg := range m.ColumnGroups {
		fmt.Printf("\n  --- Column Group #%d ---\n", i)
		fmt.Printf("  Format:  %s\n", cg.Format)
		fmt.Printf("  Columns: %v\n", cg.Columns)
		fmt.Printf("  Files (%d):\n", len(cg.Files))
		for j, f := range cg.Files {
			fmt.Printf("    [%d] Path: %s\n", j, f.Path)
			fmt.Printf("        Range: [%d, %d)\n", f.StartIndex, f.EndIndex)
			if len(f.Metadata) > 0 {
				fmt.Printf("        Metadata (%d bytes): %s\n", len(f.Metadata), hex.EncodeToString(f.Metadata))
			}
			if len(f.Properties) > 0 {
				fmt.Printf("        Properties:\n")
				for pk, pv := range f.Properties {
					fmt.Printf("          %s: %s\n", pk, pv)
				}
			}
		}
	}

	fmt.Printf("\nDelta Logs (%d):\n", len(m.DeltaLogs))
	for i, dl := range m.DeltaLogs {
		fmt.Printf("  [%d] Path: %s  Type: %s  NumEntries: %d\n", i, dl.Path, dl.Type, dl.NumEntries)
	}

	if len(m.Stats) > 0 {
		fmt.Printf("\nStats (%d keys):\n", len(m.Stats))
		for k, stat := range m.Stats {
			fmt.Printf("  %s:\n", k)
			fmt.Printf("    Paths:\n")
			for _, p := range stat.Paths {
				fmt.Printf("      - %s\n", p)
			}
			if len(stat.Metadata) > 0 {
				fmt.Printf("    Metadata:\n")
				for mk, mv := range stat.Metadata {
					fmt.Printf("      %s: %s\n", mk, mv)
				}
			}
		}
	}

	if len(m.Indexes) > 0 {
		fmt.Printf("\nIndexes (%d):\n", len(m.Indexes))
		for i, idx := range m.Indexes {
			fmt.Printf("  [%d] Column: %s  Type: %s  Path: %s\n", i, idx.ColumnName, idx.IndexType, idx.Path)
			if len(idx.Properties) > 0 {
				fmt.Printf("      Properties:\n")
				for pk, pv := range idx.Properties {
					fmt.Printf("        %s: %s\n", pk, pv)
				}
			}
		}
	}
}
