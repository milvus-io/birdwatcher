package tasks

import (
	"fmt"
	"sync"

	"go.uber.org/atomic"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/storage/common"
)

type DedupTask struct {
	baseScanTask
	limit   int64
	pkField models.FieldSchema
	// dedup
	ids         sync.Map // pk set
	dedupResult sync.Map // id => duplicated count
}

func (t *DedupTask) Scan(pk common.PrimaryKey, batchInfo *common.BatchInfo, offset int, values map[int64]any) error {
	pkv := pk.GetValue()
	_, ok := t.ids.LoadOrStore(pkv, struct{}{})
	if ok {
		t.counter.Add(1)
		v, _ := t.dedupResult.LoadOrStore(pkv, atomic.NewInt64(0))
		v.(*atomic.Int64).Inc()
	}

	return nil
}

func (t *DedupTask) Summary() {
	total := t.counter.Load()
	fmt.Printf("%d duplicated entries found\n", total)
	var i int64
	t.dedupResult.Range(func(pk, cnt any) bool {
		if i > 10 {
			return false
		}
		fmt.Printf("PK[%s] %v duplicated %d times\n", t.pkField.Name, pk, cnt.(*atomic.Int64).Load()+1)
		i++
		return true
	})
}

func NewDedupTask(limit int64, pkField models.FieldSchema) *DedupTask {
	return &DedupTask{
		limit:   limit,
		pkField: pkField,
	}
}
