package models

import (
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
)

type PartitionLoaded = ProtoWrapper[*querypb.PartitionLoadInfo]

func NewPartitionLoaded(info *querypb.PartitionLoadInfo, key string) *PartitionLoaded {
	return NewProtoWrapper(info, key)
}
