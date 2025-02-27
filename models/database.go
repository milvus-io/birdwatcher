package models

import (
	"github.com/milvus-io/milvus/pkg/v2/proto/etcdpb"
)

type Database = ProtoWrapper[*etcdpb.DatabaseInfo]

func NewDatabase(info *etcdpb.DatabaseInfo, key string) *Database {
	return NewProtoWrapper(info, key)
}
