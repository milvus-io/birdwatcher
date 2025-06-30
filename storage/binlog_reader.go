package storage

import (
	"context"
	"io"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/cockroachdb/errors"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/storage/binlog"
	"github.com/milvus-io/birdwatcher/storage/common"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

type SegmentBinlogRecordReader struct {
	common.LogBatchIterator

	storageVersion int64
	outputFields   []int64
	// index          map[int64]int

	currentBatch *common.BatchInfo
	brs          []binlog.BinlogReader
	rrs          []array.RecordReader
}

func (crr *SegmentBinlogRecordReader) iterateNextBatch(ctx context.Context) error {
	if crr.brs != nil {
		for _, er := range crr.brs {
			if er != nil {
				er.Close()
			}
		}
	}
	if crr.rrs != nil {
		for _, rr := range crr.rrs {
			if rr != nil {
				rr.Release()
			}
		}
	}

	batchInfo, err := crr.NextBatch()
	if err != nil {
		return err
	}

	crr.currentBatch = batchInfo

	crr.rrs = make([]array.RecordReader, len(batchInfo.Output))
	crr.brs = make([]binlog.BinlogReader, len(batchInfo.Output))

	var i int
	for _, readSeeker := range batchInfo.Output {
		reader, err := binlog.NewBinlogReader(crr.storageVersion, readSeeker)
		if err != nil {
			return err
		}

		rr, err := reader.NextRecordReader(ctx)
		if err != nil {
			return err
		}
		crr.rrs[i] = rr
		crr.brs[i] = reader
		i++
	}
	return nil
}

func (crr *SegmentBinlogRecordReader) Next(ctx context.Context) (common.RecordBatch, *common.BatchInfo, error) {
	if crr.rrs == nil {
		if err := crr.iterateNextBatch(ctx); err != nil {
			return nil, nil, err
		}
	}

	composeRecord := func() (common.RecordBatch, error) {
		recs := make([]arrow.Array, len(crr.outputFields))

		index := make(map[int64]int)
		for i, br := range crr.brs {
			mapping := br.GetMapping()
			rr := crr.rrs[i]

			if !rr.Next() {
				return nil, io.EOF
			}

			rec := rr.Record()
			for fieldID, idx := range mapping {
				for i, output := range crr.outputFields {
					if fieldID == output {
						recs[i] = rec.Column(idx)
						index[fieldID] = i
					}
				}
			}
		}
		return common.NewCompositeRecordBatch(index, recs), nil
	}

	// Try compose records
	r, err := composeRecord()
	if err == io.EOF {
		// if EOF, try iterate next batch (blob)
		if err := crr.iterateNextBatch(ctx); err != nil {
			return nil, nil, err
		}
		r, err = composeRecord() // try compose again
	}
	if err != nil {
		return nil, nil, err
	}
	return r, crr.currentBatch, nil
}

func (crr *SegmentBinlogRecordReader) Close() error {
	if crr.brs != nil {
		for _, er := range crr.brs {
			if er != nil {
				er.Close()
			}
		}
	}
	if crr.rrs != nil {
		for _, rr := range crr.rrs {
			if rr != nil {
				rr.Release()
			}
		}
	}
	return nil
}

func NewSegmentReader(segment *models.Segment, selectedFields []int64, translator func(binlog string) (common.ReadSeeker, error)) (*SegmentBinlogRecordReader, error) {
	var binlogSelector common.BinlogSelector
	switch segment.StorageVersion {
	case 0, 1:
		binlogSelector = common.NewFieldIDSelector(selectedFields)
	case 2:
		binlogSelector = common.NewAllSelector()
	default:
		return nil, errors.Newf("unsupported storage version: %d", segment.StorageVersion)
	}
	logIterator, err := common.NewSegmentBatchIterator(segment, binlogSelector, translator)
	if err != nil {
		return nil, err
	}

	return &SegmentBinlogRecordReader{
		LogBatchIterator: logIterator,
		outputFields:     selectedFields,
		storageVersion:   segment.StorageVersion,
	}, nil
}

func DeserializeItem(arr arrow.Array, dataType schemapb.DataType, idx int) (any, bool) {
	entry, ok := common.SerdeMap[dataType]
	if !ok {
		return nil, false
	}
	return entry.Deserialize(arr, idx)
}
