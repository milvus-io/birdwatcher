package storage

import (
	"github.com/apache/arrow/go/v8/parquet/file"

	"github.com/milvus-io/birdwatcher/proto/v2.0/schemapb"
)

type ParquetReader struct {
	reader *file.Reader
}

func NewParquetReader(colType schemapb.DataType, r ReadSeeker) (*ParquetReader, error) {
	reader, err := file.NewParquetReader(r)
	if err != nil {
		return nil, err
	}
	return &ParquetReader{
		reader: reader,
	}, nil
}

func (r *ParquetReader) Next() {
	// r.reader.RowGroup(0).Column(0)
}
