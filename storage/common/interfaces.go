package common

import (
	"io"

	"github.com/apache/arrow/go/v17/arrow"
)

// ReadSeeker is an interface that combines io.Reader, io.ReaderAt, and io.Seeker.
// It adapts bytes array, local file or any remote oss object implementing these interfaces.
type ReadSeeker interface {
	io.Reader
	io.ReaderAt
	io.Seeker
}

// RecordBatch is an interface that represents a batch of records.
type RecordBatch interface {
	Column(fieldID int64) arrow.Array
	Len() int
	Release()
	Retain()
}

// RecordReader is an interface that provides a way to read record batches.
type RecordReader interface {
	Next() (RecordBatch, error)
	Close() error
}

type LogBatchIterator interface {
	NextBatch() (*BatchInfo, error)
}
