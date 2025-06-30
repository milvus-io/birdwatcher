package storage

import (
	"context"
	"io"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/samber/lo"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/storage/binlog"
	binlogv1 "github.com/milvus-io/birdwatcher/storage/binlog/v1"
	"github.com/milvus-io/birdwatcher/storage/common"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

type SegmentBinlogRecordReader struct {
	common.LogBatchIterator

	outputFields []int64
	index        map[int64]int

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

	crr.rrs = make([]array.RecordReader, len(crr.outputFields))
	crr.brs = make([]binlog.BinlogReader, len(crr.outputFields))

	for fieldID, readSeeker := range batchInfo.Output {
		reader, err := binlogv1.NewBinlogReader(readSeeker)
		if err != nil {
			return err
		}

		i := crr.index[fieldID]
		rr, err := reader.NextRecordReader(ctx)
		if err != nil {
			return err
		}
		crr.rrs[i] = rr
		crr.brs[i] = reader
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

		for i := range crr.outputFields {
			if crr.rrs[i] != nil {
				if ok := crr.rrs[i].Next(); !ok {
					return nil, io.EOF
				}
				recs[i] = crr.rrs[i].Record().Column(0)
			}
		}
		return common.NewCompositeRecordBatch(crr.index, recs), nil
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
	// TODO adapt storage v2
	logIterator, err := common.NewSegmentBatchIterator(segment, selectedFields, translator)
	if err != nil {
		return nil, err
	}

	idx := 0
	index := lo.SliceToMap(selectedFields, func(fieldID int64) (int64, int) {
		idx++
		return fieldID, idx - 1
	})

	return &SegmentBinlogRecordReader{
		LogBatchIterator: logIterator,
		outputFields:     selectedFields,
		index:            index,
	}, nil
}

func DeserializeItem(arr arrow.Array, dataType schemapb.DataType, idx int) (any, bool) {
	entry, ok := common.SerdeMap[dataType]
	if !ok {
		return nil, false
	}
	return entry.Deserialize(arr, idx)
}
