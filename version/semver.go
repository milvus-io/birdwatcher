package version

import "github.com/golang/protobuf/proto"

// SemVer is the model for Semantic versioning
type SemVer struct {
	Major      int32  `protobuf:"varint,1,opt,name=major,proto3" json:"major,omitempty"`
	Minor      int32  `protobuf:"varint,2,opt,name=minor,proto3" json:"minor,omitempty"`
	Patch      int32  `protobuf:"varint,3,opt,name=patch,proto3" json:"patch,omitempty"`
	PreRelease string `protobuf:"bytes,4,opt,name=pre_release,proto3" json:"pre_release,omitempty"`
	Build      string `protobuf:"bytes,5,opt,name=build,proto3" json:"build,omitempty"`
}

// Reset implements protoiface.MessageV1
func (v *SemVer) Reset() {
	*v = SemVer{}
}

// String implements protoiface.MessageV1
func (v *SemVer) String() string {
	return proto.CompactTextString(v)
}

// String implements protoiface.MessageV1
func (v *SemVer) ProtoMessage() {}
