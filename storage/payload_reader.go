package storage

import (
	"bytes"
	"errors"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/parquet"
	"github.com/apache/arrow/go/v8/parquet/file"
	"github.com/milvus-io/birdwatcher/proto/v2.0/schemapb"
	"github.com/samber/lo"
)

type ParquetPayloadReader struct {
	colType schemapb.DataType
	reader  *file.Reader
}

// NewParquetPayloadReader get a PayloadReader with parquet file reader
func NewParquetPayloadReader(colType schemapb.DataType, buf []byte) (*ParquetPayloadReader, error) {
	if len(buf) == 0 {
		return nil, errors.New("create PayloadReader with empty buf")
	}

	reader, err := file.NewParquetReader(bytes.NewReader(buf))
	if err != nil {
		return nil, err
	}
	return &ParquetPayloadReader{reader: reader, colType: colType}, nil
}

func (r *ParquetPayloadReader) GetInt8FromPayLoad(colIdx int) ([]int8, error) {
	data, err := readPayloadAll[int32, *file.Int32ColumnChunkReader](r.reader, colIdx)
	if err != nil {
		return nil, err
	}
	return lo.Map(data, func(v int32, _ int) int8 {
		return int8(v)
	}), nil
}

func (r *ParquetPayloadReader) GetInt16FromPayLoad(colIdx int) ([]int16, error) {
	data, err := readPayloadAll[int32, *file.Int32ColumnChunkReader](r.reader, colIdx)
	if err != nil {
		return nil, err
	}
	return lo.Map(data, func(v int32, _ int) int16 {
		return int16(v)
	}), nil
}

func (r *ParquetPayloadReader) GetInt32FromPayLoad(colIdx int) ([]int32, error) {
	return readPayloadAll[int32, *file.Int32ColumnChunkReader](r.reader, colIdx)
}

func (r *ParquetPayloadReader) GetInt64sFromPayload(colIdx int) ([]int64, error) {
	return readPayloadAll[int64, *file.Int64ColumnChunkReader](r.reader, colIdx)
}

func (r *ParquetPayloadReader) GetStringFromPayload(colIdx int) ([]string, error) {
	byteArr, err := readPayloadAll[parquet.ByteArray, *file.ByteArrayColumnChunkReader](r.reader, colIdx)
	if err != nil {
		return nil, err
	}
	return lo.Map(byteArr, func(arr parquet.ByteArray, _ int) string {
		return arr.String()
	}), nil
}

func (r *ParquetPayloadReader) GetBytesFromPayload(colIdx int) ([]byte, error) {
	data, err := readPayloadAll[int32, *file.Int32ColumnChunkReader](r.reader, colIdx)
	if err != nil {
		return nil, err
	}
	return lo.Map(data, func(v int32, _ int) byte {
		return byte(v)
	}), nil
}

func (r *ParquetPayloadReader) GetFloat32FromPayload(colIdx int) ([]float32, error) {
	return readPayloadAll[float32, *file.Float32ColumnChunkReader](r.reader, colIdx)
}

func (r *ParquetPayloadReader) GetFloat64FromPayload(colIdx int) ([]float64, error) {
	return readPayloadAll[float64, *file.Float64ColumnChunkReader](r.reader, colIdx)
}

func (r *ParquetPayloadReader) GetBytesSliceFromPayload(colIdx int) ([][]byte, error) {
	byteArr, err := readPayloadAll[parquet.ByteArray, *file.ByteArrayColumnChunkReader](r.reader, colIdx)
	if err != nil {
		return nil, err
	}
	return lo.Map(byteArr, func(arr parquet.ByteArray, _ int) []byte {
		return arr
	}), nil
}

func (r *ParquetPayloadReader) GetFloatVectorFromPayload(colIdx int, dim int) ([][]float32, error) {
	data, err := readPayloadAll[parquet.FixedLenByteArray, *file.FixedLenByteArrayColumnChunkReader](r.reader, colIdx)
	if err != nil {
		return nil, err
	}

	return lo.Map(data, func(v parquet.FixedLenByteArray, _ int) []float32 {
		dim := v.Len() / 4
		vector := make([]float32, dim)
		copy(arrow.Float32Traits.CastToBytes(vector), v)
		return vector
	}), nil
}

func readPayloadAll[T any, Reader interface {
	file.ColumnChunkReader
	ReadBatch(int64, []T, []int16, []int16) (int64, int, error)
}](r *file.Reader, colIdx int) ([]T, error) {

	var rowCount int64
	for i := 0; i < r.NumRowGroups(); i++ {
		rowCount += r.RowGroup(i).NumRows()
	}

	result := make([]T, rowCount)

	var offset int64
	for i := 0; i < r.NumRowGroups(); i++ {
		rowGroup := r.RowGroup(i)
		groupCount := rowGroup.NumRows()

		reader, ok := rowGroup.Column(colIdx).(Reader)
		if !ok {
			return nil, errors.New("column chunk reader conversion failed")
		}
		total, read, err := reader.ReadBatch(groupCount, result[offset:rowCount], nil, nil)
		if err != nil {
			return nil, err
		}
		if total != groupCount || int64(read) != groupCount {
			return nil, errors.New("row count not matched")
		}
	}

	return result, nil
}
