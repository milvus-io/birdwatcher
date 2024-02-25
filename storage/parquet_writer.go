package storage

import (
	"encoding/binary"
	"io"
	"math"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/array"
	"github.com/apache/arrow/go/v8/arrow/memory"
	"github.com/apache/arrow/go/v8/parquet"
	"github.com/apache/arrow/go/v8/parquet/compress"
	"github.com/apache/arrow/go/v8/parquet/pqarrow"
	"github.com/cockroachdb/errors"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/samber/lo"
)

func ToArrowDataType(dataType models.DataType, dim int) arrow.DataType {
	switch dataType {
	case models.DataTypeBool:
		return &arrow.BooleanType{}
	case models.DataTypeInt8:
		return &arrow.Int8Type{}
	case models.DataTypeInt16:
		return &arrow.Int16Type{}
	case models.DataTypeInt32:
		return &arrow.Int32Type{}
	case models.DataTypeInt64:
		return &arrow.Int64Type{}
	case models.DataTypeFloat:
		return &arrow.Float32Type{}
	case models.DataTypeDouble:
		return &arrow.Float64Type{}
	case models.DataTypeString, models.DataTypeVarChar:
		return &arrow.StringType{}
	case models.DataTypeArray:
		return &arrow.BinaryType{}
	case models.DataTypeJSON:
		return &arrow.BinaryType{}
	case models.DataTypeBinaryVector:
		return &arrow.FixedSizeBinaryType{
			ByteWidth: dim / 8,
		}
	case models.DataTypeFloatVector:
		return &arrow.FixedSizeBinaryType{
			ByteWidth: dim * 4,
		}
	}
	return nil
}

type ParquetWriter struct {
	schema   *arrow.Schema
	builders map[string]array.Builder
	fields   map[string]arrow.Field
}

func NewParquetWriter(collection *models.Collection) *ParquetWriter {

	fields := lo.Map(collection.Schema.Fields, func(field models.FieldSchema, _ int) arrow.Field {
		dim, _ := field.GetDim()
		return arrow.Field{
			Name: field.Name,
			Type: ToArrowDataType(field.DataType, int(dim)),
		}
	})

	builders := lo.SliceToMap(fields, func(field arrow.Field) (string, array.Builder) {
		return field.Name, array.NewBuilder(memory.DefaultAllocator, field.Type)
	})
	fieldMap := lo.SliceToMap(fields, func(field arrow.Field) (string, arrow.Field) {
		return field.Name, field
	})

	schema := arrow.NewSchema(fields, nil)

	return &ParquetWriter{
		schema:   schema,
		builders: builders,
		fields:   fieldMap,
	}
}

func (w *ParquetWriter) AppendBool(field string, v bool) error {
	return AppendBuilder[bool, *array.BooleanBuilder](field, w.builders, v)
}

func (w *ParquetWriter) AppendInt8(field string, v int8) error {
	return AppendBuilder[int8, *array.Int8Builder](field, w.builders, v)
}

func (w *ParquetWriter) AppendInt16(field string, v int16) error {
	return AppendBuilder[int16, *array.Int16Builder](field, w.builders, v)
}

func (w *ParquetWriter) AppendInt32(field string, v int32) error {
	return AppendBuilder[int32, *array.Int32Builder](field, w.builders, v)
}

func (w *ParquetWriter) AppendInt64(field string, v int64) error {
	return AppendBuilder[int64, *array.Int64Builder](field, w.builders, v)
}

func (w *ParquetWriter) AppendFloat32(field string, v float32) error {
	return AppendBuilder[float32, *array.Float32Builder](field, w.builders, v)
}

func (w *ParquetWriter) AppendFloat64(field string, v float64) error {
	return AppendBuilder[float64, *array.Float64Builder](field, w.builders, v)
}

func (w *ParquetWriter) AppendString(field string, v string) error {
	return AppendBuilder[string, *array.StringBuilder](field, w.builders, v)
}

func (w *ParquetWriter) AppendBytes(field string, v []byte) error {
	return AppendBuilder[[]byte, *array.BinaryBuilder](field, w.builders, v)
}

func (w *ParquetWriter) AppendFloatVector(field string, vec []float32) error {
	length := len(vec)
	bytesLength := length * 4
	bytesData := make([]byte, bytesLength)

	for i := 0; i < length; i++ {
		bytes := math.Float32bits(vec[i])
		binary.LittleEndian.PutUint32(bytesData[i*4:], bytes)
	}

	return AppendBuilder[[]byte, *array.FixedSizeBinaryBuilder](field, w.builders, bytesData)
}

func (w *ParquetWriter) AppendBinaryVector(field string, vec []byte) error {
	return AppendBuilder[[]byte, *array.FixedSizeBinaryBuilder](field, w.builders, vec)
}

func (w *ParquetWriter) Flush(writer io.Writer) error {

	columns := make([]arrow.Column, 0, len(w.builders))
	arrs := make([]arrow.Array, 0, len(w.builders))

	rows := int64(0)
	for field, builder := range w.builders {
		rowCount := builder.Len()
		if rows != 0 && rows != int64(rowCount) {
			return errors.New("columns row count differs")
		}
		rows = int64(rowCount)
		arr := builder.NewArray()
		column := arrow.NewColumnFromArr(w.fields[field], arr)

		arrs = append(arrs, arr)
		columns = append(columns, column)
	}

	defer func() {
		for _, arr := range arrs {
			arr.Release()
		}
		for _, column := range columns {
			column.Release()
		}
	}()

	table := array.NewTable(w.schema, columns, rows)
	defer table.Release()

	props := parquet.NewWriterProperties(
		parquet.WithCompression(compress.Codecs.Zstd),
		parquet.WithCompressionLevel(3),
	)
	pqarrow.NewArrowWriterProperties()

	return pqarrow.WriteTable(table, writer, 1024*1024*1024, props, pqarrow.NewArrowWriterProperties())
}

func AppendBuilder[T any, Builder interface {
	AppendValues([]T, []bool)
}](field string, builders map[string]array.Builder, v T) error {
	b, ok := builders[field]
	if !ok {
		return errors.Newf("field %s builder not found", field)
	}
	builder, ok := b.(Builder)
	if !ok {
		return errors.Newf("field %s builder type not match", field)
	}

	builder.AppendValues([]T{v}, nil)
	return nil
}
