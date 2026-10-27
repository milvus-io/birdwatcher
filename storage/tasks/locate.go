package tasks

import (
	"fmt"
	"io"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/storage/common"
)

type LocateTask struct {
	baseScanTask
	limit   int64
	pkField models.FieldSchema
}

func (t *LocateTask) Scan(pk common.PrimaryKey, batchInfo *common.BatchInfo, offset int, values map[int64]any) error {
	idx := t.counter.Add(1)
	if idx > t.limit && t.limit > 0 {
		return io.EOF
	}
	fmt.Printf("entry found, segment %d offset %d, pk: %v, ts: %v\n", batchInfo.SegmentID, offset, pk.GetValue(), values[1])
	for fieldID, value := range values {
		fmt.Printf("field %d: %v\n", fieldID, value)
	}
	if batchInfo.ManifestPath != "" {
		fmt.Printf("manifest %s, segment offset %d, pk file %s, file offset %d\n", batchInfo.ManifestPath,
			int64(offset), batchInfo.TargetBinlogs[t.pkField.FieldID], batchInfo.FileOffsets[t.pkField.FieldID]+int64(offset)-batchInfo.SegmentOffset)
	} else {
		fmt.Printf("binlog batch %d, pk binlog %s\n", batchInfo.BatchIdx, batchInfo.TargetBinlogs[t.pkField.FieldID])
	}

	return nil
}

func (t *LocateTask) Summary() {}

func NewLocateTask(limit int64, pkField models.FieldSchema) *LocateTask {
	return &LocateTask{
		limit:   limit,
		pkField: pkField,
	}
}
