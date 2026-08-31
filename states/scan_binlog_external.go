package states

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"strconv"
	"strings"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/apache/arrow/go/v17/parquet/file"
	"github.com/apache/arrow/go/v17/parquet/pqarrow"
	"github.com/cockroachdb/errors"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/oss"
	"github.com/milvus-io/birdwatcher/states/ossutil"
	"github.com/milvus-io/birdwatcher/storage"
	storagecommon "github.com/milvus-io/birdwatcher/storage/common"
	"github.com/milvus-io/birdwatcher/storage/tasks"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

// scanExternalSegment scans an external collection segment by reading its
// manifest and iterating the column group files. Files are routed to the
// internal or external object store per-file (a single manifest may mix
// external data files and internal function-output files such as sparse
// vectors generated from varchar columns).
//
// filters are applied per row, matching the binlog scan path contract
// (filter.Match(pk, ts, values)).
func scanExternalSegment(ctx context.Context,
	internalStore oss.ObjectStore,
	internalRootPath string,
	externalStore oss.ObjectStore,
	externalLocation ossutil.ExternalSourceLocation,
	segment *models.Segment,
	schema *schemapb.CollectionSchema,
	fields map[int64]*schemapb.FieldSchema,
	filters []storage.EntryFilter,
	scanTask tasks.ScanTask,
) error {
	rawManifest := segment.GetManifestPath()
	if rawManifest == "" {
		return errors.Newf("external segment %d has no manifest path", segment.GetID())
	}

	var manifestRef struct {
		Ver      int    `json:"ver"`
		BasePath string `json:"base_path"`
	}
	if err := json.Unmarshal([]byte(rawManifest), &manifestRef); err != nil {
		return errors.Wrap(err, "parse manifest path JSON")
	}

	manifestBasePath := oss.ResolveObjectKey(internalRootPath, manifestRef.BasePath)
	manifestPath := path.Join(manifestBasePath, "_metadata", fmt.Sprintf("manifest-%d.avro", manifestRef.Ver))

	obj, err := internalStore.Open(ctx, manifestPath)
	if err != nil {
		return errors.Wrapf(err, "get manifest object %s", manifestPath)
	}
	if closer, ok := obj.(interface{ Close() error }); ok {
		defer closer.Close()
	}

	m, err := parseManifest(obj)
	if err != nil {
		return errors.Wrap(err, "parse manifest")
	}

	resolver := ossutil.NewManifestPathResolver(internalStore, manifestBasePath, externalStore, externalLocation)

	// fieldID => manifest column name
	field2Column := buildExternalColumnMap(schema, fields)

	pkSchema, ok := getPKSchema(schema)
	if !ok {
		return errors.New("pk field not found in schema")
	}

	var pk storagecommon.PrimaryKey
	switch pkSchema.GetDataType() {
	case schemapb.DataType_Int64:
		pk = &storagecommon.Int64PrimaryKey{}
	case schemapb.DataType_VarChar:
		pk = &storagecommon.VarCharPrimaryKey{}
	default:
		return errors.Newf("unsupported primary key type %s", pkSchema.GetDataType().String())
	}

	batchIdx := 0
	for _, cg := range m.ColumnGroups {
		// map manifest column names in this group to output field IDs
		col2Field := make(map[string]int64)
		usable := false
		for _, colName := range cg.Columns {
			if fid, ok := field2Column[colName]; ok {
				col2Field[colName] = fid
				usable = true
			}
		}
		if !usable {
			continue
		}

		for _, f := range cg.Files {
			store, objectKey, _, err := resolver.Resolve(f.Path, "_data")
			if err != nil {
				fmt.Printf("failed to resolve manifest file path %s: %s\n", f.Path, err.Error())
				continue
			}
			if err := scanExternalParquetFile(ctx, store, objectKey, col2Field, fields, pk, filters, scanTask, segment.GetID(), batchIdx); err != nil {
				fmt.Printf("failed to scan %s: %s\n", objectKey, err.Error())
				continue
			}
			batchIdx++
		}
	}
	return nil
}

// buildExternalColumnMap builds a map from manifest column name to field ID for
// all output fields. For external data columns the manifest stores the source
// column name (external_field); for internal function-output fields it stores
// the numeric field ID string.
func buildExternalColumnMap(schema *schemapb.CollectionSchema, fields map[int64]*schemapb.FieldSchema) map[string]int64 {
	result := make(map[string]int64)
	for fid, field := range fields {
		if ext := field.GetExternalField(); ext != "" {
			result[ext] = fid
			continue
		}
		result[strconv.FormatInt(fid, 10)] = fid
		// fall back to field name as well, in case the manifest uses names
		result[field.GetName()] = fid
	}
	return result
}

func getPKSchema(schema *schemapb.CollectionSchema) (*schemapb.FieldSchema, bool) {
	for _, field := range schema.GetFields() {
		if field.GetIsPrimaryKey() {
			return field, true
		}
	}
	return nil, false
}

// scanExternalParquetFile reads one parquet object and feeds every row into the
// scan task. values is keyed by field ID; PK and timestamp are set when present.
func scanExternalParquetFile(ctx context.Context,
	store oss.ObjectStore,
	objectKey string,
	col2Field map[string]int64,
	fields map[int64]*schemapb.FieldSchema,
	pk storagecommon.PrimaryKey,
	filters []storage.EntryFilter,
	scanTask tasks.ScanTask,
	segmentID int64,
	batchIdx int,
) error {
	obj, err := store.Open(ctx, objectKey)
	if err != nil {
		return errors.Wrapf(err, "open object %s", objectKey)
	}
	if closer, ok := obj.(interface{ Close() error }); ok {
		defer closer.Close()
	}

	pqReader, err := file.NewParquetReader(obj)
	if err != nil {
		return errors.Wrapf(err, "open parquet %s", objectKey)
	}
	defer pqReader.Close()

	arrReader, err := pqarrow.NewFileReader(pqReader, pqarrow.ArrowReadProperties{BatchSize: 1024}, memory.DefaultAllocator)
	if err != nil {
		return errors.Wrapf(err, "create arrow reader for %s", objectKey)
	}
	rr, err := arrReader.GetRecordReader(ctx, nil, nil)
	if err != nil {
		return errors.Wrapf(err, "get record reader for %s", objectKey)
	}
	defer rr.Release()

	batchInfo := &storagecommon.BatchInfo{
		SegmentID: segmentID,
		BatchIdx:  batchIdx,
	}

	for rr.Next() {
		rec := rr.Record()
		if rec == nil {
			continue
		}
		cols := int(rec.NumCols())
		colIdxByName := make(map[string]int, cols)
		for i := 0; i < cols; i++ {
			colIdxByName[rec.ColumnName(i)] = i
		}

		rows := int(rec.NumRows())
		for row := 0; row < rows; row++ {
			values := make(map[int64]any)
			valueSet := false
			for colName, fid := range col2Field {
				idx, ok := colIdxByName[colName]
				if !ok {
					continue
				}
				fieldSchema, ok := fields[fid]
				if !ok {
					continue
				}
				arr := rec.Column(idx)
				if arr == nil || arr.IsNull(row) {
					continue
				}
				val, ok := deserializeParquetCell(arr, row, fieldSchema.GetDataType())
				if !ok {
					continue
				}
				values[fid] = val
				valueSet = true
				if fieldSchema.GetIsPrimaryKey() {
					pk.SetValue(val)
				}
			}
			if !valueSet {
				continue
			}

			ts := int64(0)
			if v, ok := values[1]; ok {
				if tsv, ok := v.(int64); ok {
					ts = tsv
				}
			}

			matched := true
			for _, filter := range filters {
				match, err := filter.Match(pk, ts, values)
				if err != nil {
					return err
				}
				if !match {
					matched = false
					break
				}
			}
			if !matched {
				continue
			}

			if err := scanTask.Scan(pk, batchInfo, row, values); err != nil {
				return err
			}
		}
	}
	if err := rr.Err(); err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	return nil
}

// deserializeParquetCell deserializes a single cell from an arrow array using
// the common serde map when available; falls back to scalar extraction.
func deserializeParquetCell(arr arrow.Array, row int, dataType schemapb.DataType) (any, bool) {
	if entry, ok := storagecommon.SerdeMap[dataType]; ok {
		return entry.Deserialize(arr, row)
	}
	return nil, false
}

// isExternalCollection returns whether the collection schema is an external
// collection (has an external source).
func isExternalCollection(collection *models.Collection) bool {
	return collection.GetProto().GetSchema().GetExternalSource() != ""
}

// isExternalPath reports whether a path references the external source bucket
// (an absolute URI or a ROOT_PATH placeholder that the external store resolves).
func isExternalPath(filePath string) bool {
	trimmed := strings.TrimSpace(filePath)
	return strings.Contains(trimmed, "://") || strings.Contains(trimmed, "ROOT_PATH")
}
