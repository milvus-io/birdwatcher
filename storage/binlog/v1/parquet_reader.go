package binlogv1

import (
	"github.com/apache/arrow/go/v17/parquet/file"

	"github.com/milvus-io/birdwatcher/storage/common"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

type ParquetReader struct {
	reader *file.Reader
}

func NewParquetReader(colType schemapb.DataType, r common.ReadSeeker) (*ParquetReader, error) {
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
