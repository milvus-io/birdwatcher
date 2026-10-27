package storage

import (
	"context"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/apache/arrow/go/v17/parquet/file"
	"github.com/apache/arrow/go/v17/parquet/pqarrow"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/storage/common"
	"github.com/milvus-io/birdwatcher/storage/manifest"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

// ManifestSegmentReader joins independently chunked column groups by logical
// row position. It owns one Arrow batch per selected group, never the segment.
type ManifestSegmentReader struct {
	segmentID    int64
	manifestPath string
	reference    manifest.Reference
	fields       map[int64]*schemapb.FieldSchema
	groups       []*manifestGroupCursor
	absent       []int64
	rows         int64
	position     int64
	current      common.RecordBatch
}

// NewManifestSegmentReader reads internal Parquet v3 segments. open must resolve
// data paths relative to the manifest's _data directory before opening them.
func NewManifestSegmentReader(segment *models.Segment, m *manifest.Manifest, fields map[int64]*schemapb.FieldSchema,
	open func(string) (common.ReadSeeker, error),
) (*ManifestSegmentReader, error) {
	ref, err := manifest.ParseReference(segment.GetManifestPath())
	if err != nil {
		return nil, err
	}
	r := &ManifestSegmentReader{segmentID: segment.GetID(), manifestPath: segment.GetManifestPath(), reference: ref, fields: fields}
	present := make(map[int64]bool)
	var rows int64 = -1
	for groupIdx, group := range m.ColumnGroups {
		var groupRows int64
		for _, f := range group.Files {
			if f.StartIndex < 0 || f.EndIndex < f.StartIndex || strings.TrimSpace(f.Path) == "" {
				return nil, fmt.Errorf("segment %d column group %d has invalid file range/path: %+v", segment.GetID(), groupIdx, f)
			}
			n := f.EndIndex - f.StartIndex
			if n > math.MaxInt64-groupRows {
				return nil, fmt.Errorf("segment %d column group %d row count overflows", segment.GetID(), groupIdx)
			}
			groupRows += n
		}
		if rows >= 0 && rows != groupRows {
			return nil, fmt.Errorf("segment %d column group %d has %d rows, expected %d", segment.GetID(), groupIdx, groupRows, rows)
		}
		rows = groupRows
		selected := make(map[string]int64)
		for _, name := range group.Columns {
			fid, err := strconv.ParseInt(name, 10, 64)
			if err != nil || fields[fid] == nil {
				continue
			}
			if present[fid] {
				return nil, fmt.Errorf("segment %d has duplicate manifest column %s", segment.GetID(), name)
			}
			present[fid] = true
			selected[name] = fid
		}
		if len(selected) == 0 {
			continue
		}
		if !strings.EqualFold(group.Format, "parquet") {
			return nil, fmt.Errorf("segment %d column group %d: unsupported storage v3 format %q (supported: parquet)", segment.GetID(), groupIdx, group.Format)
		}
		r.groups = append(r.groups, &manifestGroupCursor{groupID: groupIdx, files: group.Files, selected: selected, open: open})
	}
	if rows < 0 {
		rows = 0
	}
	r.rows = rows
	// SegmentInfo row counts can lag while a segment is growing. The manifest
	// snapshot, including its column-group ranges, is the source of truth.
	if rows == 0 && segment.GetNumOfRows() > 0 && len(m.ColumnGroups) == 0 {
		return nil, fmt.Errorf("segment %d has %d rows but no manifest column groups", segment.GetID(), segment.GetNumOfRows())
	}
	for fid, field := range fields {
		if !present[fid] {
			if field.GetIsPrimaryKey() || fid == 1 {
				if rows > 0 {
					return nil, fmt.Errorf("segment %d manifest is missing required field %d", segment.GetID(), fid)
				}
			} else {
				if rows > 0 && !field.GetNullable() && field.GetDefaultValue() == nil {
					return nil, fmt.Errorf("segment %d manifest is missing non-nullable field %d without a default", segment.GetID(), fid)
				}
				r.absent = append(r.absent, fid)
			}
		}
	}
	return r, nil
}

// Next returns a borrowed batch, valid until the next Next or Close. Call
// Retain when holding a batch beyond that point, as with the legacy reader.
func (r *ManifestSegmentReader) Next(ctx context.Context) (common.RecordBatch, *common.BatchInfo, error) {
	if r.current != nil {
		r.current.Release()
		r.current = nil
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if r.position >= r.rows {
		return nil, nil, io.EOF
	}
	n := int64(1024)
	n = min(n, r.rows-r.position)
	for _, group := range r.groups {
		if err := group.ensure(ctx); err != nil {
			if err == io.EOF {
				err = io.ErrUnexpectedEOF
			}
			return nil, nil, fmt.Errorf("segment %d at row %d: %w", r.segmentID, r.position, err)
		}
		n = min(n, group.record.NumRows()-group.offset)
		n = min(n, group.file.EndIndex-group.fileRow)
	}
	common.ScanDebug(ctx, "manifest.batch.ready segment_offset=%d rows=%d total_rows=%d", r.position, n, r.rows)
	info := &common.BatchInfo{
		SegmentID: r.segmentID, ManifestPath: r.manifestPath, SegmentOffset: r.position,
		TargetBinlogs: make(map[int64]string), FileOffsets: make(map[int64]int64),
	}
	index := make(map[int64]int)
	var cols []arrow.Array
	for _, group := range r.groups {
		for fid, col := range group.mapping {
			index[fid] = len(cols)
			cols = append(cols, array.NewSlice(group.record.Column(col), group.offset, group.offset+n))
			info.TargetBinlogs[fid], _ = r.reference.FilePath("_data", group.file.Path)
			info.FileOffsets[fid] = group.fileRow
		}
		group.offset += n
		group.fileRow += n
	}
	for _, fid := range r.absent {
		col, err := makeAbsentField(r.fields[fid], int(n))
		if err != nil {
			for _, c := range cols {
				c.Release()
			}
			return nil, nil, err
		}
		index[fid] = len(cols)
		cols = append(cols, col)
	}
	r.position += n
	r.current = common.NewCompositeRecordBatch(index, cols)
	return r.current, info, nil
}

func (r *ManifestSegmentReader) Close() error {
	if r.current != nil {
		r.current.Release()
		r.current = nil
	}
	var err error
	for _, group := range r.groups {
		if closeErr := group.closeFile(); err == nil {
			err = closeErr
		}
	}
	return err
}

type manifestGroupCursor struct {
	groupID  int
	files    []manifest.ColumnGroupFile
	selected map[string]int64
	open     func(string) (common.ReadSeeker, error)
	nextFile int
	file     manifest.ColumnGroupFile
	pq       *file.Reader
	rr       pqarrow.RecordReader
	record   arrow.Record
	mapping  map[int64]int
	offset   int64
	fileRow  int64
}

func (g *manifestGroupCursor) closeFile() error {
	g.record = nil
	if g.rr != nil {
		g.rr.Release()
		g.rr = nil
	}
	if g.pq != nil {
		err := g.pq.Close()
		g.pq = nil
		return err
	}
	return nil
}

func (g *manifestGroupCursor) openNext(ctx context.Context) error {
	if err := g.closeFile(); err != nil {
		return err
	}
	for g.nextFile < len(g.files) {
		g.file = g.files[g.nextFile]
		g.nextFile++
		if g.file.StartIndex == g.file.EndIndex {
			common.ScanDebug(ctx, "manifest.file.skip file=%q reason=empty_range", common.ScanLogPath(g.file.Path))
			continue
		}
		obj, err := g.open(g.file.Path)
		if err != nil {
			return fmt.Errorf("open manifest data %s: %w", g.file.Path, err)
		}
		buffered, err := common.NewBufferedReadSeeker(obj, common.DefaultReadBufferSize)
		if err != nil {
			if closer, ok := obj.(io.Closer); ok {
				closer.Close()
			}
			return fmt.Errorf("buffer manifest data %s: %w", g.file.Path, err)
		}
		obj = buffered
		end := common.ScanOperation(ctx, "parquet.metadata file=%q range=[%d,%d)", common.ScanLogPath(g.file.Path), g.file.StartIndex, g.file.EndIndex)
		g.pq, err = file.NewParquetReader(obj)
		end(err)
		if err != nil {
			if closer, ok := obj.(io.Closer); ok {
				closer.Close()
			}
			g.pq = nil
			return fmt.Errorf("open parquet %s: %w", g.file.Path, err)
		}
		if g.file.EndIndex > g.pq.NumRows() {
			return fmt.Errorf("manifest range [%d, %d) exceeds %d rows in %s", g.file.StartIndex, g.file.EndIndex, g.pq.NumRows(), g.file.Path)
		}
		ar, err := pqarrow.NewFileReader(g.pq, pqarrow.ArrowReadProperties{BatchSize: 1024}, memory.DefaultAllocator)
		if err != nil {
			return err
		}
		schema, err := ar.Schema()
		if err != nil {
			return err
		}
		// pqarrow projects Parquet leaf indices. Resolve each selected top-level
		// field's full leaf range, so list/nested fields retain their structure.
		var leaves []int
		var names []string
		for _, f := range schema.Fields() {
			if _, ok := g.selected[f.Name]; ok {
				names = append(names, f.Name)
			}
		}
		if len(names) != len(g.selected) {
			return fmt.Errorf("file %s is missing manifest columns %v", g.file.Path, g.selected)
		}
		for i := 0; i < g.pq.MetaData().Schema.NumColumns(); i++ {
			root := g.pq.MetaData().Schema.Column(i).ColumnPath()[0]
			if _, ok := g.selected[root]; ok {
				leaves = append(leaves, i)
			}
		}
		end = common.ScanOperation(ctx, "parquet.project file=%q leaves=%v row_groups=%d", common.ScanLogPath(g.file.Path), leaves, g.pq.NumRowGroups())
		g.rr, err = ar.GetRecordReader(ctx, leaves, nil)
		end(err)
		if err != nil {
			return err
		}
		g.mapping = make(map[int64]int)
		for i, f := range g.rr.Schema().Fields() {
			fid, ok := g.selected[f.Name]
			if !ok {
				return fmt.Errorf("unexpected projected column %s in %s", f.Name, g.file.Path)
			}
			g.mapping[fid] = i
		}
		g.fileRow = 0
		g.offset = 0
		common.ScanDebug(ctx, "manifest.file.ready file=%q file_index=%d file_rows=%d selected=%v", common.ScanLogPath(g.file.Path), g.nextFile-1, g.pq.NumRows(), g.mapping)
		return nil
	}
	return io.EOF
}

func (g *manifestGroupCursor) ensure(ctx context.Context) error {
	ctx = common.WithScanScope(ctx, "column_group=%d", g.groupID)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if g.rr == nil || g.fileRow >= g.file.EndIndex {
			if err := g.openNext(ctx); err != nil {
				return err
			}
		}
		if g.record == nil || g.offset >= g.record.NumRows() {
			if !common.NextScanRecord(ctx, g.rr, "parquet.arrow.next file=%q file_row=%d", common.ScanLogPath(g.file.Path), g.fileRow) {
				if err := g.rr.Err(); err != nil && err != io.EOF {
					return err
				}
				return fmt.Errorf("unexpected end of parquet %s at row %d, expected range end %d", g.file.Path, g.fileRow, g.file.EndIndex)
			}
			g.record = g.rr.Record()
			g.offset = 0
		}
		if g.fileRow < g.file.StartIndex {
			skip := min(g.file.StartIndex-g.fileRow, g.record.NumRows()-g.offset)
			g.offset += skip
			g.fileRow += skip
			continue
		}
		if g.record.NumRows() == 0 {
			continue
		}
		return nil
	}
}

func makeAbsentField(field *schemapb.FieldSchema, n int) (arrow.Array, error) {
	entry, ok := common.SerdeMap[field.GetDataType()]
	if !ok {
		return nil, fmt.Errorf("unsupported absent field %d type %s", field.GetFieldID(), field.GetDataType())
	}
	var dim int
	for _, p := range field.GetTypeParams() {
		if p.GetKey() == "dim" {
			dim, _ = strconv.Atoi(p.GetValue())
		}
	}
	b := array.NewBuilder(memory.DefaultAllocator, entry.ArrowType(dim))
	defer b.Release()
	var value any
	if v := field.GetDefaultValue(); v != nil {
		switch field.GetDataType() {
		case schemapb.DataType_Bool:
			value = v.GetBoolData()
		case schemapb.DataType_Int8:
			value = int8(v.GetIntData())
		case schemapb.DataType_Int16:
			value = int16(v.GetIntData())
		case schemapb.DataType_Int32:
			value = v.GetIntData()
		case schemapb.DataType_Int64:
			value = v.GetLongData()
		case schemapb.DataType_Float:
			value = v.GetFloatData()
		case schemapb.DataType_Double:
			value = v.GetDoubleData()
		case schemapb.DataType_String, schemapb.DataType_VarChar:
			value = v.GetStringData()
		default:
			return nil, fmt.Errorf("unsupported default for field %d type %s", field.GetFieldID(), field.GetDataType())
		}
	}
	b.Reserve(n)
	for range n {
		if !entry.Serialize(b, value) {
			return nil, fmt.Errorf("cannot fill absent field %d", field.GetFieldID())
		}
	}
	return b.NewArray(), nil
}
