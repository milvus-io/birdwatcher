package models

import (
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

type ChannelWatch = ProtoWrapper[*datapb.ChannelWatchInfo]

func NewChannelWatch(info *datapb.ChannelWatchInfo, key string) *ChannelWatch {
	return NewProtoWrapper(info, key)
}
