package models

import (
	"strconv"
	"strings"
	"sync"

	"github.com/milvus-io/birdwatcher/proto/v2.0/commonpb"
	"github.com/milvus-io/birdwatcher/proto/v2.0/etcdpb"
	"github.com/milvus-io/birdwatcher/proto/v2.0/schemapb"
	commonpbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/commonpb"
	etcdpbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/etcdpb"
	schemapbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/schemapb"
	"github.com/samber/lo"
)

// Collection model for collection information.
type Collection struct {
	ID int64
	// TODO partitions
	Schema           CollectionSchema
	CreateTime       uint64
	Channels         []Channel
	ShardsNum        int32
	ConsistencyLevel ConsistencyLevel
	State            CollectionState
	Properties       map[string]string
	DBID             int64

	// etcd collection key
	key string

	// lazy load func
	loadOnce sync.Once
	lazyLoad func(*Collection)
}

func (c *Collection) Key() string {
	return c.key
}

// CollectionHistory collection models with extra history data.
type CollectionHistory struct {
	Collection
	Ts      uint64
	Dropped bool
}

func (c *Collection) GetPKField() (FieldSchema, bool) {
	for _, field := range c.Schema.Fields {
		if field.IsPrimaryKey {
			return field, true
		}
	}
	return FieldSchema{}, false
}

func (c *Collection) GetVectorField() (FieldSchema, bool) {
	for _, field := range c.Schema.Fields {
		if field.DataType == DataTypeBinaryVector || field.DataType == DataTypeFloatVector {
			return field, true
		}
	}
	return FieldSchema{}, false
}

// newCollectionFromBase fetchs common base information form proto objects
func newCollectionFromBase[collectionBase interface {
	GetID() int64
	GetCreateTime() uint64
	GetShardsNum() int32
	GetPhysicalChannelNames() []string
	GetVirtualChannelNames() []string
	GetStartPositions() []KD
}, KD interface {
	GetKey() string
	GetData() []byte
}](info collectionBase) *Collection {
	c := &Collection{}

	c.ID = info.GetID()
	c.CreateTime = info.GetCreateTime()
	c.ShardsNum = info.GetShardsNum()
	c.Channels = getChannels(info.GetPhysicalChannelNames(), info.GetVirtualChannelNames(), info.GetStartPositions())

	return c
}

// NewCollectionFrom2_1 parses etcdpb.CollectionInfo(proto v2.0) to models.Collection.
func NewCollectionFromV2_1(info *etcdpb.CollectionInfo, key string) *Collection {
	c := newCollectionFromBase[*etcdpb.CollectionInfo, *commonpb.KeyDataPair](info)
	c.key = key
	schema := info.GetSchema()
	c.Schema = newSchemaFromBase(schema)
	c.Schema.Fields = lo.Map(schema.GetFields(), func(fieldSchema *schemapb.FieldSchema, _ int) FieldSchema {
		fs := NewFieldSchemaFromBase[*schemapb.FieldSchema, schemapb.DataType](fieldSchema)
		fs.Properties = GetMapFromKVPairs(fieldSchema.GetTypeParams())
		return fs
	})
	// hard code created for version <= v2.1.4
	c.State = CollectionStateCollectionCreated
	c.ConsistencyLevel = ConsistencyLevel(info.GetConsistencyLevel())

	return c
}

// NewCollectionFromV2_2 parses etcdpb.CollectionInfo(proto v2.2) to models.Collections.
func NewCollectionFromV2_2(info *etcdpbv2.CollectionInfo, key string, fields []*schemapbv2.FieldSchema) *Collection {
	c := newCollectionFromBase[*etcdpbv2.CollectionInfo, *commonpbv2.KeyDataPair](info)
	c.key = key
	c.DBID = parseDBID(key)
	c.State = CollectionState(info.GetState())
	schema := info.GetSchema()
	schema.Fields = fields
	c.Schema = newSchemaFromBase(schema)

	c.Schema.Fields = lo.Map(fields, func(fieldSchema *schemapbv2.FieldSchema, _ int) FieldSchema {
		fs := NewFieldSchemaFromBase[*schemapbv2.FieldSchema, schemapbv2.DataType](fieldSchema)
		fs.Properties = GetMapFromKVPairs(fieldSchema.GetTypeParams())
		fs.IsDynamic = fieldSchema.GetIsDynamic()
		fs.IsPartitionKey = fieldSchema.GetIsPartitionKey()
		return fs
	})
	c.Schema.EnableDynamicSchema = info.GetSchema().GetEnableDynamicField()

	c.ConsistencyLevel = ConsistencyLevel(info.GetConsistencyLevel())
	info.GetStartPositions()

	c.Properties = make(map[string]string)
	for _, prop := range info.GetProperties() {
		c.Properties[prop.GetKey()] = prop.GetValue()
	}

	return c
}

func parseDBID(key string) int64 {
	parts := strings.Split(key, "/")
	if len(parts) < 2 {
		return 0
	}
	id, err := strconv.ParseInt(parts[len(parts)-2], 10, 64)
	if err != nil {
		return 0
	}
	return id
}

func getChannels[cp interface {
	GetKey() string
	GetData() []byte
}](pcs, vcs []string, cps []cp) []Channel {
	return lo.Map(cps, func(c cp, idx int) Channel {
		return Channel{
			PhysicalName: pcs[idx],
			VirtualName:  vcs[idx],
			StartPosition: &MsgPosition{
				ChannelName: c.GetKey(),
				MsgID:       c.GetData(),
			},
		}
	})
}

// GetMapFromKVPairs parses kv pairs to map[string]string.
func GetMapFromKVPairs[kvPair interface {
	GetKey() string
	GetValue() string
}](pairs []kvPair) map[string]string {
	result := make(map[string]string)
	for _, kv := range pairs {
		result[kv.GetKey()] = kv.GetValue()
	}
	return result
}
