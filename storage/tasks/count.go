package tasks

import (
	"fmt"

	"github.com/milvus-io/birdwatcher/storage/common"
)

type CountTask struct {
	baseScanTask
}

func (t *CountTask) Scan(pk common.PrimaryKey, batchInfo *common.BatchInfo, offset int, values map[int64]any) error {
	t.counter.Add(1)
	return nil
}

func (t *CountTask) Summary() {
	fmt.Printf("Total %d entries found\n", t.counter.Load())
}

func NewCountTask() *CountTask {
	return &CountTask{}
}
