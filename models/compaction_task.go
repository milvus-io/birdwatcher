package models

import (
	"github.com/milvus-io/birdwatcher/proto/v2.2/datapb"
)

/*
	PlanID               int64                      `protobuf:"varint,1,opt,name=planID,proto3" json:"planID,omitempty"`
	TriggerID            int64                      `protobuf:"varint,2,opt,name=triggerID,proto3" json:"triggerID,omitempty"`
	CollectionID         int64                      `protobuf:"varint,3,opt,name=collectionID,proto3" json:"collectionID,omitempty"`
	PartitionID          int64                      `protobuf:"varint,4,opt,name=partitionID,proto3" json:"partitionID,omitempty"`
	Channel              string                     `protobuf:"bytes,5,opt,name=channel,proto3" json:"channel,omitempty"`
	Type                 CompactionType             `protobuf:"varint,6,opt,name=type,proto3,enum=milvus.proto.data.CompactionType" json:"type,omitempty"`
	State                CompactionTaskState        `protobuf:"varint,7,opt,name=state,proto3,enum=milvus.proto.data.CompactionTaskState" json:"state,omitempty"`
	FailReason           string                     `protobuf:"bytes,8,opt,name=fail_reason,json=failReason,proto3" json:"fail_reason,omitempty"`
	StartTime            int64                      `protobuf:"varint,9,opt,name=start_time,json=startTime,proto3" json:"start_time,omitempty"`
	EndTime              int64                      `protobuf:"varint,10,opt,name=end_time,json=endTime,proto3" json:"end_time,omitempty"`
	TimeoutInSeconds     int32                      `protobuf:"varint,11,opt,name=timeout_in_seconds,json=timeoutInSeconds,proto3" json:"timeout_in_seconds,omitempty"`
	RetryTimes           int32                      `protobuf:"varint,12,opt,name=retry_times,json=retryTimes,proto3" json:"retry_times,omitempty"`
	CollectionTtl        int64                      `protobuf:"varint,13,opt,name=collection_ttl,json=collectionTtl,proto3" json:"collection_ttl,omitempty"`
	TotalRows            int64                      `protobuf:"varint,14,opt,name=total_rows,json=totalRows,proto3" json:"total_rows,omitempty"`
	InputSegments        []int64                    `protobuf:"varint,15,rep,packed,name=inputSegments,proto3" json:"inputSegments,omitempty"`
	ResultSegments       []int64                    `protobuf:"varint,16,rep,packed,name=resultSegments,proto3" json:"resultSegments,omitempty"`
	Pos                  *msgpb.MsgPosition         `protobuf:"bytes,17,opt,name=pos,proto3" json:"pos,omitempty"`
	NodeID               int64                      `protobuf:"varint,18,opt,name=nodeID,proto3" json:"nodeID,omitempty"`
	Schema               *schemapb.CollectionSchema `protobuf:"bytes,19,opt,name=schema,proto3" json:"schema,omitempty"`
	ClusteringKeyField   *schemapb.FieldSchema      `protobuf:"bytes,20,opt,name=clustering_key_field,json=clusteringKeyField,proto3" json:"clustering_key_field,omitempty"`
	MaxSegmentRows       int64                      `protobuf:"varint,21,opt,name=max_segment_rows,json=maxSegmentRows,proto3" json:"max_segment_rows,omitempty"`
	PreferSegmentRows    int64                      `protobuf:"varint,22,opt,name=prefer_segment_rows,json=preferSegmentRows,proto3" j
*/

// CompactionTask model for collection compaction task information.
type CompactionTask struct {
	*datapb.CompactionTask
	// etcd collection key
	key string
}

func (ct *CompactionTask) Key() string {
	return ct.key
}

// CollectionHistory collection models with extra history data.
type CompactionTaskHistory struct {
	CompactionTask
	Ts      int64
	Dropped bool
}

//func (ct *CompactionTask) GetClusterKeyField() (FieldSchema, bool) {
//	return ct.clusterFieldSchema, true
//}

// NewCompactionTask parses compactionTask(proto v2.2) to models.CompactionTask
func NewCompactionTask(info *datapb.CompactionTask, key string) *CompactionTask {
	ct := &CompactionTask{
		CompactionTask: info,
		key:            key,
	}
	// fs := NewFieldSchemaFromBase[*schemapbv2.FieldSchema, schemapbv2.DataType](info.GetClusteringKeyField())
	// fs.Properties = GetMapFromKVPairs(fieldSchema.GetTypeParams())
	// fs.IsDynamic = fieldSchema.GetIsDynamic()
	// fs.IsPartitionKey = fieldSchema.GetIsPartitionKey()
	// ct.clusterFieldSchema = fs
	return ct
}
