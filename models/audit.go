package models

import "github.com/golang/protobuf/proto"

// AuditHeader stores birdwatcher audit log header info.
type AuditHeader struct {
	// Version number for audit format
	Version int32 `protobuf:"varint,1,opt,name=version,proto3" json:"version,omitempty"`
	// OpType enum for operation type
	OpType int32 `protobuf:"varint,2,opt,name=op_type,proto3" json:"op_type,omitempty"`
	// EntriesNum following entries number
	EntriesNum int32 `protobuf:"varint,3,opt,name=entries_num,proto3" json:"entries_num,omitempty"`
}

// Reset implements protoiface.MessageV1
func (v *AuditHeader) Reset() {
	*v = AuditHeader{}
}

// String implements protoiface.MessageV1
func (v *AuditHeader) String() string {
	return proto.CompactTextString(v)
}

// String implements protoiface.MessageV1
func (v *AuditHeader) ProtoMessage() {}

// AuditOpType operation enum type for audit log.
type AuditOpType int32

const (
	// OpDel operation type for delete.
	OpDel AuditOpType = 1
	// OpPut operation type for put.
	OpPut AuditOpType = 2
	// OpPutBefore sub header for put before value.
	OpPutBefore AuditOpType = 3
	// OpPutAfter sub header for put after value.
	OpPutAfter AuditOpType = 4
)
