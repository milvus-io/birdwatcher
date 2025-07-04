package common

import (
	"fmt"

	"github.com/apache/arrow/go/v17/arrow"
)

var _ RecordBatch = (*compositeRecordBatch)(nil)

// compositeRecordBatch is a record being composed of array
type compositeRecordBatch struct {
	index map[int64]int
	recs  []arrow.Array
}

var _ RecordBatch = (*compositeRecordBatch)(nil)

func (r *compositeRecordBatch) Column(i int64) arrow.Array {
	if _, ok := r.index[i]; !ok {
		return nil
	}
	return r.recs[r.index[i]]
}

func (r *compositeRecordBatch) Len() int {
	if len(r.recs) == 0 {
		fmt.Println("empty record batch found")
		return 0
	}
	return r.recs[0].Len()
}

func (r *compositeRecordBatch) Release() {
	for _, rec := range r.recs {
		rec.Release()
	}
}

func (r *compositeRecordBatch) Retain() {
	for _, rec := range r.recs {
		rec.Retain()
	}
}

func NewCompositeRecordBatch(index map[int64]int, recs []arrow.Array) RecordBatch {
	return &compositeRecordBatch{
		index: index,
		recs:  recs,
	}
}

type simpleArrowRecord struct {
	r arrow.Record

	field2Col map[int64]int
}

var _ RecordBatch = (*simpleArrowRecord)(nil)

func (sr *simpleArrowRecord) Column(i int64) arrow.Array {
	colIdx, ok := sr.field2Col[i]
	if !ok {
		panic("no such field")
	}
	return sr.r.Column(colIdx)
}

func (sr *simpleArrowRecord) Len() int {
	if sr.r == nil {
		fmt.Println("empty record batch found")
		return 0
	}
	return int(sr.r.NumRows())
}

func (sr *simpleArrowRecord) Release() {
	sr.r.Release()
}

func (sr *simpleArrowRecord) Retain() {
	sr.r.Retain()
}

func (sr *simpleArrowRecord) ArrowSchema() *arrow.Schema {
	return sr.r.Schema()
}

func NewSimpleArrowRecord(r arrow.Record, field2Col map[int64]int) *simpleArrowRecord {
	return &simpleArrowRecord{
		r:         r,
		field2Col: field2Col,
	}
}
