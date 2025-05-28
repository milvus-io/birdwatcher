package models

import (
	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/etcdpb"
)

type Collection struct {
	*ProtoWrapper[*etcdpb.CollectionInfo]
	Functions []*Function
}

func (c *Collection) WithFields(fields []*schemapb.FieldSchema) {
	c.ProtoWrapper.GetProto().Schema.Fields = fields
}

func (c *Collection) Channels() []*Channel {
	return lo.Map(c.proto.VirtualChannelNames, func(virtualName string, idx int) *Channel {
		return &Channel{
			PhysicalName: c.proto.PhysicalChannelNames[idx],
			VirtualName:  virtualName,
			StartPosition: &msgpb.MsgPosition{
				ChannelName: c.proto.StartPositions[idx].GetKey(),
				MsgID:       c.proto.StartPositions[idx].GetData(),
			},
		}
	})
}

func NewCollection(info *etcdpb.CollectionInfo, key string) *Collection {
	return &Collection{
		ProtoWrapper: NewProtoWrapper(info, key),
	}
}

// CollectionHistory collection models with extra history data.
type CollectionHistory struct {
	*Collection
	Ts      uint64
	Dropped bool
}

func (c *Collection) GetPKField() (FieldSchema, bool) {
	for _, field := range c.GetProto().Schema.Fields {
		if field.IsPrimaryKey {
			return NewFieldSchemaFromBase(field), true
		}
	}
	return FieldSchema{}, false
}

func (c *Collection) GetVectorField() (FieldSchema, bool) {
	for _, field := range c.GetProto().Schema.Fields {
		if field.DataType == schemapb.DataType_BinaryVector || field.DataType == schemapb.DataType_FloatVector {
			return NewFieldSchemaFromBase(field), true
		}
	}
	return FieldSchema{}, false
}
