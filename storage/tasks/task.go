package tasks

import (
	"go.uber.org/atomic"

	"github.com/milvus-io/birdwatcher/storage/common"
)

type ScanTask interface {
	Scan(pk common.PrimaryKey, batchInfo *common.BatchInfo, offset int, values map[int64]any) error
	Counter() int64
	Summary()
}

type baseScanTask struct {
	counter atomic.Int64
}

func (b *baseScanTask) Counter() int64 {
	return b.counter.Load()
}
