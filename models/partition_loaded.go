package models

import (
	querypbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/querypb"
)

type PartitionLoaded struct {
	CollectionID  int64
	PartitionID   int64
	ReplicaNumber int32

	Status LoadStatus

	Key     string
	Version string
}

func NewPartitionLoaded(info *querypbv2.PartitionLoadInfo, key string) *PartitionLoaded {
	return &PartitionLoaded{
		CollectionID:  info.GetCollectionID(),
		PartitionID:   info.GetPartitionID(),
		ReplicaNumber: info.GetReplicaNumber(),
		Status:        LoadStatus(info.GetStatus()),
		Version:       GTEVersion2_2,
		Key:           key,
	}
}
