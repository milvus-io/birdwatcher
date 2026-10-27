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
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

// SegmentRecordReader returns borrowed batches valid until the next Next or Close.
type SegmentRecordReader interface {
	Next(context.Context) (common.RecordBatch, *common.BatchInfo, error)
	Close() error
}

type SegmentBinlogRecordReader struct {
	common.LogBatchIterator

	storageVersion int64
	outputFields   []int64
	// index          map[int64]int

	currentBatch *common.BatchInfo
	brs          []binlog.BinlogReader
	rrs          []array.RecordReader
}

func (crr *SegmentBinlogRecordReader) nextRecordReader(ctx context.Context) error {
	if len(crr.brs) == 0 {
		return io.EOF
	}

	for i, br := range crr.brs {
		end := common.ScanOperation(ctx, "legacy.arrow.open reader=%d", i)
		cr, err := br.NextRecordReader(ctx)
		end(err)
		if err != nil {
			return err
		}

		if crr.rrs[i] != nil {
			crr.rrs[i].Release()
		}
		crr.rrs[i] = cr
	}
	return nil
}

func (crr *SegmentBinlogRecordReader) iterateNext(ctx context.Context) error {
	// try to get next record reader from current batch
	err := crr.nextRecordReader(ctx)
	if err == nil {
		return nil
	}

	// not EOF error, return it
	if !errors.Is(err, io.EOF) {
		return err
	}

	// EOF, iterator to next batch
	if err := crr.Close(); err != nil {
		return err
	}

	end := common.ScanOperation(ctx, "legacy.next_file_batch")
	batchInfo, err := crr.NextBatch()
	end(err)
	if err != nil {
		return err
	}

	crr.currentBatch = batchInfo

	crr.rrs = make([]array.RecordReader, 0, len(batchInfo.Output))
	crr.brs = make([]binlog.BinlogReader, 0, len(batchInfo.Output))

	for groupID, readSeeker := range batchInfo.Output {
		end := common.ScanOperation(ctx, "legacy.reader.init batch=%d field_group=%d storage_version=%d", batchInfo.BatchIdx, groupID, crr.storageVersion)
		reader, err := binlog.NewBinlogReader(crr.storageVersion, readSeeker)
		end(err)
		if err != nil {
			return err
		}

		mapping := reader.GetMapping()
		hit := false
		for _, outputField := range crr.outputFields {
			if _, ok := mapping[outputField]; ok {
				hit = true
				break
			}
		}
		// all fields are filtered, ignore this binlog
		if !hit {
			continue
		}

		// select columns if possible
		reader.SelectFields(crr.outputFields)

		end = common.ScanOperation(ctx, "legacy.arrow.open batch=%d field_group=%d fields=%v", batchInfo.BatchIdx, groupID, crr.outputFields)
		rr, err := reader.NextRecordReader(ctx)
		end(err)
		if err != nil {
			return err
		}
		crr.rrs = append(crr.rrs, rr)
		crr.brs = append(crr.brs, reader)
	}
	return nil
}

func (crr *SegmentBinlogRecordReader) Next(ctx context.Context) (common.RecordBatch, *common.BatchInfo, error) {
	if crr.rrs == nil {
		if err := crr.iterateNext(ctx); err != nil {
			return nil, nil, err
		}
	}

	composeRecord := func() (common.RecordBatch, error) {
		recs := make([]arrow.Array, len(crr.outputFields))

		index := make(map[int64]int)
		var recIdx int
		for i, br := range crr.brs {
			mapping := br.GetMapping()
			rr := crr.rrs[i]

			if !common.NextScanRecord(ctx, rr, "legacy.arrow.next batch=%d reader=%d", crr.currentBatch.BatchIdx, i) {
				if err := rr.Err(); err != nil && !errors.Is(err, io.EOF) {
					return nil, err
				}
				return nil, io.EOF
			}

			rec := rr.Record()
			for fieldID, idx := range mapping {
				for _, output := range crr.outputFields {
					if fieldID == output {
						recs[recIdx] = rec.Column(idx)
						index[fieldID] = recIdx
						recIdx++
					}
				}
			}
		}
		common.ScanDebug(ctx, "legacy.batch.ready batch=%d fields=%v", crr.currentBatch.BatchIdx, index)
		return common.NewCompositeRecordBatch(index, recs), nil
	}

	// Try compose records
	r, err := composeRecord()
	if err == io.EOF {
		// if EOF, try iterate next batch (blob)
		if err := crr.iterateNext(ctx); err != nil {
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
	crr.brs = nil
	crr.rrs = nil
	var err error
	if crr.currentBatch != nil {
		for _, source := range crr.currentBatch.Output {
			if closer, ok := source.(io.Closer); ok {
				if closeErr := closer.Close(); err == nil {
					err = closeErr
				}
			}
		}
		crr.currentBatch = nil
	}
	return err
}

func NewSegmentReader(segment *models.Segment, selectedFields []int64, translator func(binlog string) (common.ReadSeeker, error)) (*SegmentBinlogRecordReader, error) {
	var binlogSelector common.BinlogSelector
	switch segment.StorageVersion {
	case 0, 1:
		binlogSelector = common.NewFieldIDSelector(selectedFields)
	case 2:
		binlogSelector = common.NewMatchingWideColumnSelector(selectedFields)
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
