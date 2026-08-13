package states

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

type manifestTestBinaryEncoder struct {
	bytes.Buffer
}

const manifestTestSchemaV6 = `{
  "type": "record",
  "name": "Manifest",
  "namespace": "milvus_storage",
  "fields": [
    {"name": "column_groups", "type": {"type": "array", "items": {
      "type": "record", "name": "ColumnGroup", "fields": [
        {"name": "columns", "type": {"type": "array", "items": "string"}},
        {"name": "files", "type": {"type": "array", "items": {
          "type": "record", "name": "ColumnGroupFile", "fields": [
            {"name": "path", "type": "string"},
            {"name": "start_index", "type": "long", "default": 0},
            {"name": "end_index", "type": "long", "default": 0},
            {"name": "properties", "type": {"type": "map", "values": "string"}, "default": {}}
          ]
        }}},
        {"name": "format", "type": "string"}
      ]
    }}},
    {"name": "delta_logs", "type": {"type": "array", "items": {
      "type": "record", "name": "DeltaLog", "fields": [
        {"name": "path", "type": "string"},
        {"name": "type", "type": "int"},
        {"name": "num_entries", "type": "long"}
      ]
    }}},
    {"name": "stats", "type": {"type": "map", "values": {
      "type": "record", "name": "Statistics", "fields": [
        {"name": "paths", "type": {"type": "array", "items": "string"}},
        {"name": "metadata", "type": {"type": "map", "values": "string"}, "default": {}}
      ]
    }}, "default": {}},
    {"name": "indexes", "type": {"type": "array", "items": {
      "type": "record", "name": "Index", "fields": [
        {"name": "column_name", "type": "string"},
        {"name": "index_name", "type": "string", "default": ""},
        {"name": "index_type", "type": "string"},
        {"name": "path", "type": "string"},
        {"name": "field_id", "type": "long", "default": 0},
        {"name": "index_id", "type": "long", "default": 0},
        {"name": "build_id", "type": "long", "default": 0},
        {"name": "index_version", "type": "long", "default": 0},
        {"name": "num_rows", "type": "long", "default": 0},
        {"name": "serialized_size", "type": "long", "default": 0},
        {"name": "mem_size", "type": "long", "default": 0},
        {"name": "current_index_version", "type": "int", "default": 0},
        {"name": "current_scalar_index_version", "type": "int", "default": 0},
        {"name": "index_store_path_version", "type": "int", "default": 0},
        {"name": "index_file_keys", "type": {"type": "array", "items": "string"}, "default": []},
        {"name": "properties", "type": {"type": "map", "values": "string"}, "default": {}}
      ]
    }}, "default": []},
    {"name": "lob_files", "type": {"type": "array", "items": {
      "type": "record", "name": "LobFileInfo", "fields": [
        {"name": "path", "type": "string"},
        {"name": "field_id", "type": "long"},
        {"name": "total_rows", "type": "long"},
        {"name": "valid_rows", "type": "long"},
        {"name": "file_size_bytes", "type": "long"}
      ]
    }}, "default": []}
  ]
}`

func (e *manifestTestBinaryEncoder) writeLong(value int64) {
	encoded := uint64(value<<1) ^ uint64(value>>63)
	for encoded >= 0x80 {
		e.WriteByte(byte(encoded) | 0x80)
		encoded >>= 7
	}
	e.WriteByte(byte(encoded))
}

func (e *manifestTestBinaryEncoder) writeString(value string) {
	e.writeLong(int64(len(value)))
	e.WriteString(value)
}

func (e *manifestTestBinaryEncoder) writeStringArray(values []string) {
	if len(values) > 0 {
		e.writeLong(int64(len(values)))
		for _, value := range values {
			e.writeString(value)
		}
	}
	e.writeLong(0)
}

func (e *manifestTestBinaryEncoder) writeStringMap(values map[string]string) {
	if len(values) > 0 {
		e.writeLong(int64(len(values)))
		for key, value := range values {
			e.writeString(key)
			e.writeString(value)
		}
	}
	e.writeLong(0)
}

func (e *manifestTestBinaryEncoder) writeBytes(value []byte) {
	e.writeLong(int64(len(value)))
	e.Write(value)
}

func TestParseAvroOCFManifestV6(t *testing.T) {
	var record manifestTestBinaryEncoder
	writeManifestTestOCFPrefix(&record)
	writeManifestTestIndexV6(&record)
	writeManifestTestLobFile(&record)

	m := parseManifestTestOCF(t, record.Bytes())
	require.Equal(t, int32(6), m.Version)
	require.Equal(t, map[string]string{"file_size": "1024"}, m.ColumnGroups[0].Files[0].Properties)
	require.Len(t, m.Indexes, 1)
	require.Equal(t, manifestIndex{
		ColumnName:                "embedding",
		IndexName:                 "embedding_hnsw",
		IndexType:                 "HNSW",
		Path:                      "embedding_hnsw",
		FieldID:                   101,
		IndexID:                   102,
		BuildID:                   103,
		IndexVersion:              104,
		NumRows:                   105,
		SerializedSize:            106,
		MemSize:                   107,
		CurrentIndexVersion:       108,
		CurrentScalarIndexVersion: 109,
		IndexStorePathVersion:     110,
		IndexFileKeys:             []string{"index_params", "data"},
		Properties:                map[string]string{"M": "16", "metric_type": "COSINE"},
	}, m.Indexes[0])
	require.Len(t, m.LobFiles, 1)
}

func TestParseLegacyManifestV6(t *testing.T) {
	var data manifestTestBinaryEncoder
	data.writeLong(int64(manifestMagic))
	data.writeLong(manifestVersionV6)
	writeManifestTestLegacyPrefix(&data)
	writeManifestTestIndexV6(&data)
	writeManifestTestLobFile(&data)

	m, err := parseManifest(bytes.NewReader(data.Bytes()))
	require.NoError(t, err)
	require.Equal(t, int32(6), m.Version)
	require.Equal(t, []byte{1, 2}, m.ColumnGroups[0].Files[0].Metadata)
	require.Len(t, m.Indexes, 1)
	require.Equal(t, "embedding_hnsw", m.Indexes[0].IndexName)
	require.Equal(t, []string{"index_params", "data"}, m.Indexes[0].IndexFileKeys)
	require.Len(t, m.LobFiles, 1)
	require.Equal(t, int64(2048), m.LobFiles[0].FileSizeBytes)
}

func writeManifestTestOCFPrefix(data *manifestTestBinaryEncoder) {
	data.writeLong(1)
	data.writeStringArray([]string{"embedding"})
	data.writeLong(1)
	data.writeString("data.parquet")
	data.writeLong(0)
	data.writeLong(105)
	data.writeStringMap(map[string]string{"file_size": "1024"})
	data.writeLong(0)
	data.writeString("parquet")
	data.writeLong(0)
	data.writeLong(0) // delta_logs
	data.writeLong(0) // stats
}

func writeManifestTestLegacyPrefix(data *manifestTestBinaryEncoder) {
	data.writeLong(1)
	data.writeStringArray([]string{"embedding"})
	data.writeLong(1)
	data.writeString("data.parquet")
	data.writeLong(0)
	data.writeLong(105)
	data.writeBytes([]byte{1, 2})
	data.writeLong(0)
	data.writeString("parquet")
	data.writeLong(0)
	data.writeLong(0) // delta_logs
	data.writeLong(0) // stats
}

func writeManifestTestIndexV6(data *manifestTestBinaryEncoder) {
	data.writeLong(1)
	data.writeString("embedding")
	data.writeString("embedding_hnsw")
	data.writeString("HNSW")
	data.writeString("embedding_hnsw")
	for value := int64(101); value <= 110; value++ {
		data.writeLong(value)
	}
	data.writeStringArray([]string{"index_params", "data"})
	data.writeStringMap(map[string]string{"M": "16", "metric_type": "COSINE"})
	data.writeLong(0)
}

func writeManifestTestLobFile(data *manifestTestBinaryEncoder) {
	data.writeLong(1)
	data.writeString("101/_data/lob.vortex")
	data.writeLong(101)
	data.writeLong(105)
	data.writeLong(100)
	data.writeLong(2048)
	data.writeLong(0)
}

func parseManifestTestOCF(t *testing.T, record []byte) *manifest {
	t.Helper()

	var data manifestTestBinaryEncoder
	data.Write(avroOCFMagic)
	data.writeLong(2)
	data.writeString("avro.schema")
	data.writeBytes([]byte(manifestTestSchemaV6))
	data.writeString("avro.codec")
	data.writeBytes([]byte("null"))
	data.writeLong(0)
	syncMarker := []byte("0123456789abcdef")
	data.Write(syncMarker)
	data.writeLong(1)
	data.writeLong(int64(len(record)))
	data.Write(record)
	data.Write(syncMarker)

	m, err := parseManifest(bytes.NewReader(data.Bytes()))
	require.NoError(t, err)
	return m
}
