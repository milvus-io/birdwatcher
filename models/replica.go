package models

// Replica model struct for replica info.
type Replica struct {
	ID            int64
	CollectionID  int64
	NodeIDs       []int64
	ResourceGroup string
	Version       string
	ShardReplicas []ShardReplica
}

// ShardReplica information struct for shard replica in v2.1.
type ShardReplica struct {
	LeaderID   int64
	LeaderAddr string
	NodeIDs    []int64
}
