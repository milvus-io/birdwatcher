package models

import (
	"github.com/milvus-io/milvus/pkg/v2/proto/etcdpb"
)

type Partition = ProtoWrapper[*etcdpb.PartitionInfo]

func NewPartition(info *etcdpb.PartitionInfo, key string) *Partition {
	return NewProtoWrapper(info, key)
}
