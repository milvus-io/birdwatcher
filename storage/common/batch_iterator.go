package common

import (
	"io"

	"github.com/samber/lo"

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
	segment      *models.Segment
	targetFields map[int64]*models.FieldBinlog

	translator func(binlog string) (ReadSeeker, error)

	currentBatch int
}

func (sbi *SegmentBatchIterator) NextBatch() (batchInfo *BatchInfo, err error) {
	output := make(map[int64]ReadSeeker)
	targetBinlogs := make(map[int64]string)
	for fieldID, fieldBinlog := range sbi.targetFields {
		if sbi.currentBatch >= len(fieldBinlog.Binlogs) {
			return nil, io.EOF
		}
		rs, err := sbi.translator(fieldBinlog.Binlogs[sbi.currentBatch].LogPath)
		if err != nil {
			return nil, err
		}

		output[fieldID] = rs
		targetBinlogs[fieldID] = fieldBinlog.Binlogs[sbi.currentBatch].LogPath
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

func NewSegmentBatchIterator(segment *models.Segment, selectedFields []int64, translator func(binlog string) (ReadSeeker, error)) (*SegmentBatchIterator, error) {
	targets := lo.SliceToMap(selectedFields, func(fieldID int64) (int64, *models.FieldBinlog) {
		field, ok := lo.Find(segment.GetBinlogs(), func(f *models.FieldBinlog) bool {
			return f.FieldID == fieldID
		})
		if !ok {
			return fieldID, nil
		}
		return fieldID, field
	})

	return &SegmentBatchIterator{
		segment:      segment,
		targetFields: targets,

		translator: translator,
	}, nil
}
