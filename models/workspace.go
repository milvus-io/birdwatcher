package models

import "github.com/golang/protobuf/proto"

// WorkspaceMeta stores birdwatcher workspace information.
type WorkspaceMeta struct {
	// Version semver for workspace meta
	Version string `protobuf:"bytes,1,opt,name=version,proto3" json:"version,omitempty"`
	// instance name, as rootPath for key prefix
	Instance string `protobuf:"bytes,2,opt,name=instance,proto3" json:"instance,omitempty"`
	// MetaPath used in keys
	MetaPath string `protobuf:"bytes,3,opt,name=meta_path,proto3" json:"meta_path,omitempty"`
}

// Reset implements protoiface.MessageV1
func (v *WorkspaceMeta) Reset() {
	*v = WorkspaceMeta{}
}

// String implements protoiface.MessageV1
func (v *WorkspaceMeta) String() string {
	return proto.CompactTextString(v)
}

// String implements protoiface.MessageV1
func (v *WorkspaceMeta) ProtoMessage() {}
