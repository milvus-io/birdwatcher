package models

import (
	etcdpbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/etcdpb"
)

type Partition struct {
	ID           int64
	CollectionID int64
	Name         string
	State        PartitionState

	key string
}

func NewPartition(info *etcdpbv2.PartitionInfo, key string) *Partition {
	p := &Partition{
		ID:           info.GetPartitionID(),
		CollectionID: info.GetCollectionId(),
		Name:         info.GetPartitionName(),
		State:        PartitionState(info.GetState()),
		key:          key,
	}
	return p
}

type PartitionState int32

const (
	PartitionStatePartitionCreated  PartitionState = 0
	PartitionStatePartitionCreating PartitionState = 1
	PartitionStatePartitionDropping PartitionState = 2
	PartitionStatePartitionDropped  PartitionState = 3
)

var PartitionStatename = map[int32]string{
	0: "PartitionCreated",
	1: "PartitionCreating",
	2: "PartitionDropping",
	3: "PartitionDropped",
}

var PartitionStatevalue = map[string]int32{
	"PartitionCreated":  0,
	"PartitionCreating": 1,
	"PartitionDropping": 2,
	"PartitionDropped":  3,
}

func (x PartitionState) String() string {
	return EnumName(PartitionStatename, int32(x))
}
