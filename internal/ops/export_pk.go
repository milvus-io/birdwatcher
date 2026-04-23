package ops

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/apache/arrow/go/v17/parquet"
	"github.com/apache/arrow/go/v17/parquet/compress"
	"github.com/apache/arrow/go/v17/parquet/pqarrow"

	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/milvusclient"
)

type ExportPKParams struct {
	Collection string `yaml:"collection"`
	Output     string `yaml:"output"`
	Filter     string `yaml:"filter,omitempty"`
	BatchSize  int    `yaml:"batch_size,omitempty"`
}

type ExportPKResult struct {
	RowCount int64  `yaml:"row_count"`
	Output   string `yaml:"output"`
	PKField  string `yaml:"pk_field"`
	PKType   string `yaml:"pk_type"`
}

func (p *ExportPKParams) Execute(ctx context.Context, rc *RunContext) (any, error) {
	if rc.Client == nil {
		return nil, errNoClient("export_pk")
	}
	if p.Collection == "" {
		return nil, fmt.Errorf("export_pk: `collection` required")
	}
	if p.Output == "" {
		return nil, fmt.Errorf("export_pk: `output` required")
	}

	coll, err := rc.Client.DescribeCollection(ctx, milvusclient.NewDescribeCollectionOption(p.Collection))
	if err != nil {
		return nil, fmt.Errorf("describe collection: %w", err)
	}
	var pk *entity.Field
	for _, f := range coll.Schema.Fields {
		if f.PrimaryKey {
			pk = f
			break
		}
	}
	if pk == nil {
		return nil, fmt.Errorf("collection %q has no primary key", p.Collection)
	}
	if pk.DataType != entity.FieldTypeInt64 && pk.DataType != entity.FieldTypeVarChar {
		return nil, fmt.Errorf("unsupported pk type %s (only int64/varchar)", pk.DataType.String())
	}

	batch := p.BatchSize
	if batch <= 0 {
		batch = 1000
	}

	opt := milvusclient.NewQueryIteratorOption(p.Collection).
		WithOutputFields(pk.Name).
		WithBatchSize(batch)
	if p.Filter != "" {
		opt = opt.WithFilter(p.Filter)
	}
	iter, err := rc.Client.QueryIterator(ctx, opt)
	if err != nil {
		return nil, fmt.Errorf("create query iterator: %w", err)
	}

	var arrowField arrow.Field
	switch pk.DataType {
	case entity.FieldTypeInt64:
		arrowField = arrow.Field{Name: pk.Name, Type: arrow.PrimitiveTypes.Int64}
	case entity.FieldTypeVarChar:
		arrowField = arrow.Field{Name: pk.Name, Type: arrow.BinaryTypes.String}
	}
	schema := arrow.NewSchema([]arrow.Field{arrowField}, nil)

	f, err := os.Create(p.Output)
	if err != nil {
		return nil, fmt.Errorf("create output: %w", err)
	}
	defer f.Close()

	writer, err := pqarrow.NewFileWriter(schema, f,
		parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Snappy)),
		pqarrow.DefaultWriterProps())
	if err != nil {
		return nil, fmt.Errorf("create parquet writer: %w", err)
	}

	mem := memory.NewGoAllocator()
	var total int64
	for {
		rs, err := iter.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			writer.Close()
			return nil, fmt.Errorf("iterate: %w", err)
		}
		if rs.ResultCount == 0 {
			continue
		}
		col := rs.GetColumn(pk.Name)
		if col == nil {
			writer.Close()
			return nil, fmt.Errorf("pk column %q missing in result", pk.Name)
		}
		rec, err := buildPKRecord(mem, schema, pk.DataType, col, rs.ResultCount)
		if err != nil {
			writer.Close()
			return nil, err
		}
		if err := writer.Write(rec); err != nil {
			rec.Release()
			writer.Close()
			return nil, fmt.Errorf("write parquet: %w", err)
		}
		rec.Release()
		total += int64(rs.ResultCount)
	}

	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("close parquet: %w", err)
	}

	fmt.Fprintf(rc.Out(), "exported %d primary keys (%s) to %s\n", total, pk.DataType.String(), p.Output)
	return ExportPKResult{RowCount: total, Output: p.Output, PKField: pk.Name, PKType: pk.DataType.String()}, nil
}

func buildPKRecord(mem memory.Allocator, schema *arrow.Schema, dt entity.FieldType, col interface {
	GetAsInt64(int) (int64, error)
	GetAsString(int) (string, error)
	Len() int
}, n int,
) (arrow.Record, error) {
	switch dt {
	case entity.FieldTypeInt64:
		b := array.NewInt64Builder(mem)
		defer b.Release()
		b.Reserve(n)
		for i := 0; i < n; i++ {
			v, err := col.GetAsInt64(i)
			if err != nil {
				return nil, fmt.Errorf("read int64 pk at %d: %w", i, err)
			}
			b.Append(v)
		}
		arr := b.NewInt64Array()
		defer arr.Release()
		return array.NewRecord(schema, []arrow.Array{arr}, int64(n)), nil
	case entity.FieldTypeVarChar:
		b := array.NewStringBuilder(mem)
		defer b.Release()
		b.Reserve(n)
		for i := 0; i < n; i++ {
			v, err := col.GetAsString(i)
			if err != nil {
				return nil, fmt.Errorf("read varchar pk at %d: %w", i, err)
			}
			b.Append(v)
		}
		arr := b.NewStringArray()
		defer arr.Release()
		return array.NewRecord(schema, []arrow.Array{arr}, int64(n)), nil
	}
	return nil, fmt.Errorf("unsupported pk type %s", dt.String())
}

func init() {
	Register("export_pk", func() Op { return &ExportPKParams{} })
}
