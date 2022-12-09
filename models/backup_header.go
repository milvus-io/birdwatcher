package models

import "github.com/golang/protobuf/proto"

// BackupHeader stores birdwatcher backup header information.
type BackupHeader struct {
	// Version number for backup format
	Version int32 `protobuf:"varint,1,opt,name=version,proto3" json:"version,omitempty"`
	// instance name, as rootPath for key prefix
	Instance string `protobuf:"bytes,2,opt,name=instance,proto3" json:"instance,omitempty"`
	// MetaPath used in keys
	MetaPath string `protobuf:"bytes,3,opt,name=meta_path,proto3" json:"meta_path,omitempty"`
	// Entries record number of key-value in backup
	Entries int64 `protobuf:"varint,4,opt,name=entries,proto3" json:"entries,omitempty"`
	// Component is the backup target
	Component string `protobuf:"bytes,5,opt,name=component,proto3" json:"component,omitempty"`
	// Extra property reserved
	Extra []byte `protobuf:"bytes,6,opt,name=extra,proto3" json:"-"`
}

// Reset implements protoiface.MessageV1
func (v *BackupHeader) Reset() {
	*v = BackupHeader{}
}

// String implements protoiface.MessageV1
func (v *BackupHeader) String() string {
	return proto.CompactTextString(v)
}

// String implements protoiface.MessageV1
func (v *BackupHeader) ProtoMessage() {}

// PartType enum type for PartHeader.ParType.
type PartType int32

const (
	// EtcdBackup part stores Etcd KV backup.
	EtcdBackup PartType = 1
	// MetricsBackup metrics from /metrics.
	MetricsBackup PartType = 2
	// MetricsDefaultBackup metrics from /metrics_default.
	MetricsDefaultBackup PartType = 3
	// Configurations configuration fetched from milvus server.
	Configurations PartType = 4
	// AppMetrics is metrics fetched via grpc metrics api.
	AppMetrics PartType = 5
	// LoadedSegments is segment info fetched from querynode(Milvus2.2+).
	LoadedSegments PartType = 6
)

// PartHeader stores backup part information.
// Added since backup version 2.
type PartHeader struct {
	// PartType represent next part type.
	PartType int32 `protobuf:"varint,1,opt,name=part_type,proto3" json:"version,omitempty"`
	// PartLen stands for part length in bytes.
	// -1 for not sure.
	// used for fast skipping one part.
	PartLen int64 `protobuf:"varint,2,opt,name=part_len,proto3" json:"entries,omitempty"`
	// Extra used for extra info storage.
	Extra []byte `protobuf:"bytes,3,opt,name=extra,proto3" json:"-"`
}

// Reset implements protoiface.MessageV1
func (v *PartHeader) Reset() {
	*v = PartHeader{}
}

// String implements protoiface.MessageV1
func (v *PartHeader) String() string {
	return proto.CompactTextString(v)
}

// String implements protoiface.MessageV1
func (v *PartHeader) ProtoMessage() {}
