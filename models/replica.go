package models

import "github.com/milvus-io/milvus/pkg/v2/proto/querypb"

type Replica = ProtoWrapper[*querypb.Replica]

func NewReplica(info *querypb.Replica, key string) *Replica {
	return NewProtoWrapper(info, key)
}

// // Replica model struct for replica info.
// type Replica struct {
// 	ID            int64
// 	CollectionID  int64
// 	NodeIDs       []int64
// 	ResourceGroup string
// 	Version       string
// 	ShardReplicas []ShardReplica
// }

// // ShardReplica information struct for shard replica in v2.1.
// type ShardReplica struct {
// 	LeaderID   int64
// 	LeaderAddr string
// 	NodeIDs    []int64
// }
