package states

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"sort"
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
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// scanExternalSegment scans an external collection segment by reading its
// manifest and merging the column-group files by global row offset into
// logical rows. A single manifest may mix external data files and internal
// function-output files (e.g. sparse vectors generated from varchar columns),
// each covering overlapping row ranges in separate column groups.
//
// The virtual PK (segmentID + global row offset) is derived rather than read
// from any file. Filters are applied per logical row, matching the binlog scan
// path contract (filter.Match(pk, ts, values)), and Scan is invoked exactly
// once per logical row.
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

	// fieldID => manifest column name for the output fields
	field2Column := buildExternalColumnMap(schema, fields)

	// Resolve every manifest column-group file and its store up front. Any
	// resolution failure (e.g. invalid ARN, missing bucket) aborts the scan
	// instead of silently producing partial results.
	//
	// Each column group's files tile the segment's logical row space: within a
	// group, file rows are attributed to segment-global offsets by accumulating
	// the manifest row ranges ([start, end) within the file). Different column
	// groups (external data vs internal function outputs) independently cover
	// the same [0, totalRows) and are merged by segment-global offset below.
	type resolvedFile struct {
		store        oss.ObjectStore
		key          string
		col2Field    map[string]int64
		startIdx     int64 // file-local start row (manifest range)
		endIdx       int64 // file-local end row (manifest range)
		globalOffset int64 // segment-global start offset of this file's rows
	}
	var files []resolvedFile
	for _, cg := range m.ColumnGroups {
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
		var groupOffset int64
		for _, f := range cg.Files {
			store, objectKey, _, err := resolver.Resolve(f.Path, "_data")
			if err != nil {
				return errors.Wrapf(err, "resolve manifest file path %s", f.Path)
			}
			files = append(files, resolvedFile{
				store:        store,
				key:          objectKey,
				col2Field:    col2Field,
				startIdx:     f.StartIndex,
				endIdx:       f.EndIndex,
				globalOffset: groupOffset,
			})
			if f.EndIndex > f.StartIndex {
				groupOffset += f.EndIndex - f.StartIndex
			}
		}
	}
	if len(files) == 0 {
		return nil
	}

	// Scan every file and merge rows by segment-global offset, so overlapping
	// column groups contribute their columns to the same logical rows.
	valuesByOffset := make(map[int64]map[int64]any)
	for _, rf := range files {
		if err := scanExternalParquetFile(ctx, rf.store, rf.key, rf.col2Field, fields, rf.startIdx, rf.endIdx, rf.globalOffset, valuesByOffset); err != nil {
			return err
		}
	}

	offsets := make([]int64, 0, len(valuesByOffset))
	for offset := range valuesByOffset {
		offsets = append(offsets, offset)
	}
	sort.Slice(offsets, func(i, j int) bool { return offsets[i] < offsets[j] })

	pkSchema, ok := getPKSchema(schema)
	if !ok {
		return errors.New("pk field not found in schema")
	}
	virtualPK := pkSchema.GetName() == common.VirtualPKFieldName

	var pk storagecommon.PrimaryKey
	switch pkSchema.GetDataType() {
	case schemapb.DataType_Int64:
		pk = &storagecommon.Int64PrimaryKey{}
	case schemapb.DataType_VarChar:
		pk = &storagecommon.VarCharPrimaryKey{}
	default:
		return errors.Newf("unsupported primary key type %s", pkSchema.GetDataType().String())
	}

	batchInfo := &storagecommon.BatchInfo{
		SegmentID: segment.GetID(),
	}
	for _, offset := range offsets {
		values := valuesByOffset[offset]

		if virtualPK {
			pk.SetValue(typeutil.GetVirtualPK(segment.GetID(), offset))
		} else {
			pkv, ok := values[pkSchema.GetFieldID()]
			if !ok {
				return errors.Newf("primary key field %s not found for row %d of segment %d", pkSchema.GetName(), offset, segment.GetID())
			}
			pk.SetValue(pkv)
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

		if err := scanTask.Scan(pk, batchInfo, int(offset), values); err != nil {
			return err
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

// scanExternalParquetFile reads one parquet object and accumulates, per
// segment-global row offset, the requested field values keyed by field ID.
//
// The manifest entry carries a file-local row window [startIdx, endIdx): the
// same physical parquet can be split into multiple fragments, each contributing
// a disjoint window. Only rows in that window are attributed to the segment,
// starting at segment-global offset globalOffset (the file's tiled position
// within its column group). Null cells are represented as `fieldID: nil` (not
// omitted); a non-null cell that cannot be deserialized to the requested Milvus
// type returns an explicit error instead of being silently dropped.
func scanExternalParquetFile(ctx context.Context,
	store oss.ObjectStore,
	objectKey string,
	col2Field map[string]int64,
	fields map[int64]*schemapb.FieldSchema,
	startIdx, endIdx, globalOffset int64,
	valuesByOffset map[int64]map[int64]any,
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

	fileRow := int64(0)
	globalRow := globalOffset
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
			if fileRow < startIdx {
				fileRow++
				continue
			}
			if fileRow >= endIdx {
				return nil
			}
			values := valuesByOffset[globalRow]
			if values == nil {
				values = make(map[int64]any)
				valuesByOffset[globalRow] = values
			}
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
				if arr == nil {
					continue
				}
				if arr.IsNull(row) {
					values[fid] = nil
					continue
				}
				val, ok := deserializeParquetCell(arr, row, fieldSchema.GetDataType())
				if !ok {
					return errors.Newf(
						"cannot deserialize non-null cell for field %s (id %d, type %s) at row %d of %s",
						fieldSchema.GetName(), fid, fieldSchema.GetDataType().String(), row, objectKey)
				}
				values[fid] = val
			}
			fileRow++
			globalRow++
		}
	}
	if err := rr.Err(); err != nil && !errors.Is(err, io.EOF) {
		return errors.Wrapf(err, "read parquet %s", objectKey)
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
