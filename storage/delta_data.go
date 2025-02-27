package storage

import (
	"errors"
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

type DeltaData struct {
	pks      PrimaryKeys
	tss      []uint64
	dataType schemapb.DataType

	// stats
	delRowCount int64
	memSize     int64
}

func NewDeltaData(dataType schemapb.DataType, cap int) *DeltaData {
	var pks PrimaryKeys
	switch dataType {
	case schemapb.DataType_Int64:
		pks = NewInt64PrimaryKeys(cap)
	case schemapb.DataType_VarChar:
		pks = NewVarcharPrimaryKeys(cap)
	default:
		fmt.Println("data type", dataType.String(), "not supported")
	}
	return &DeltaData{
		pks:      pks,
		tss:      make([]uint64, 0, cap),
		dataType: dataType,
	}
}

func (dd *DeltaData) Append(pk PrimaryKey, ts uint64) {
	dd.pks.MustAppend(pk)
	dd.tss = append(dd.tss, ts)
	dd.delRowCount++
}

func (dd *DeltaData) Range(f func(pk PrimaryKey, ts uint64) bool) {
	for i := 0; i < int(dd.delRowCount); i++ {
		if !f(dd.pks.Get(i), dd.tss[i]) {
			return
		}
	}
}

func (dd *DeltaData) Merge(add *DeltaData) error {
	if dd.dataType != add.dataType {
		return errors.New("data type not match")
	}
	dd.pks.MustMerge(add.pks)
	dd.tss = append(dd.tss, add.tss...)
	dd.delRowCount += add.delRowCount
	return nil
}
