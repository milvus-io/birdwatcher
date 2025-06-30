package binlog

import (
	"context"

	"github.com/apache/arrow/go/v17/parquet/pqarrow"
	"github.com/cockroachdb/errors"

	binlogv1 "github.com/milvus-io/birdwatcher/storage/binlog/v1"
	"github.com/milvus-io/birdwatcher/storage/common"
)

type BinlogReader interface {
	NextRecordReader(context.Context) (pqarrow.RecordReader, error)
	Close()
}

func NewBinlogReader(storageVersion int64, f common.ReadSeeker) (BinlogReader, error) {
	switch storageVersion {
	case 0, 1:
		return binlogv1.NewBinlogReader(f)
	case 2:
		// TODO handle storage v2
		return nil, errors.Newf("unsupported storage version %d", storageVersion)
	}
	return nil, errors.Newf("unsupported storage version %d", storageVersion)
}
