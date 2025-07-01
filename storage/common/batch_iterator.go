package common

import (
	"github.com/milvus-io/birdwatcher/models"
)

type BatchInfo struct {
	SegmentID int64
	BatchIdx  int

	// field/groupID => ReadSeeker
	Output map[int64]ReadSeeker
	// field/groupID => binlog path
	TargetBinlogs map[int64]string
}

type SegmentBatchIterator struct {
	segment        *models.Segment
	binlogSelector BinlogSelector

	translator func(binlog string) (ReadSeeker, error)

	currentBatch int
}

func (sbi *SegmentBatchIterator) NextBatch() (batchInfo *BatchInfo, err error) {
	output := make(map[int64]ReadSeeker)
	targetBinlogs := make(map[int64]string)

	targetFields, err := sbi.binlogSelector.SelectBinlogs(sbi.segment.GetBinlogs(), sbi.currentBatch)
	if err != nil {
		return nil, err
	}
	for fieldID, binlogPath := range targetFields {
		rs, err := sbi.translator(binlogPath)
		if err != nil {
			return nil, err
		}

		output[fieldID] = rs
		targetBinlogs[fieldID] = binlogPath
	}

	info := &BatchInfo{
		SegmentID:     sbi.segment.ID,
		BatchIdx:      sbi.currentBatch,
		Output:        output,
		TargetBinlogs: targetBinlogs,
	}
	sbi.currentBatch++
	return info, nil
}

func NewSegmentBatchIterator(segment *models.Segment, selector BinlogSelector, translator func(binlog string) (ReadSeeker, error)) (*SegmentBatchIterator, error) {
	return &SegmentBatchIterator{
		segment:        segment,
		binlogSelector: selector,

		translator: translator,
	}, nil
}
