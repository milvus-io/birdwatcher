package models

import "github.com/milvus-io/milvus/pkg/v2/proto/datapb"

type Snapshot = ProtoWrapper[*datapb.SnapshotInfo]

func NewSnapshot(info *datapb.SnapshotInfo, key string) *Snapshot {
	return NewProtoWrapper(info, key)
}
