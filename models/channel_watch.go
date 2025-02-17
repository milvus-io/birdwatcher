package models

import (
	"github.com/samber/lo"

	datapbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/datapb"
	"github.com/milvus-io/birdwatcher/proto/v2.2/schemapb"
)

type ChannelWatch struct {
	Vchan     VChannelInfo
	StartTs   int64
	State     ChannelWatchState
	TimeoutTs int64

	// 2.4 only
	Progress int32
	OpID     int64

	// key
	key    string
	Schema *CollectionSchema

	VchanV2Pb *datapbv2.VchannelInfo
}

func (c *ChannelWatch) Key() string {
	return c.key
}

type VChannelInfo struct {
	CollectionID        int64
	ChannelName         string
	SeekPosition        *MsgPosition
	UnflushedSegmentIds []int64
	FlushedSegmentIds   []int64
	DroppedSegmentIds   []int64
}

type vchannelInfoBase interface {
	GetCollectionID() int64
	GetChannelName() string
	GetUnflushedSegmentIds() []int64
	GetFlushedSegmentIds() []int64
	GetDroppedSegmentIds() []int64
}

func GetChannelWatchInfo[ChannelWatchBase interface {
	GetVchan() vchan
	GetStartTs() int64
	GetState() watchState
	GetTimeoutTs() int64
}, watchState ~int32, vchan interface {
	vchannelInfoBase
	GetSeekPosition() pos
}, pos msgPosBase](info ChannelWatchBase, key string) *ChannelWatch {
	return &ChannelWatch{
		Vchan:     getVChannelInfo[vchan, pos](info.GetVchan()),
		StartTs:   info.GetStartTs(),
		State:     ChannelWatchState(info.GetState()),
		TimeoutTs: info.GetTimeoutTs(),
		key:       key,
	}
}

func GetChannelWatchInfoV2[ChannelWatchBase interface {
	GetVchan() *datapbv2.VchannelInfo
	GetStartTs() int64
	GetState() watchState
	GetTimeoutTs() int64
	GetSchema() *schemapb.CollectionSchema
	GetProgress() int32
	GetOpID() int64
}, watchState ~int32, pos msgPosBase](info ChannelWatchBase, key string) *ChannelWatch {
	var schema *CollectionSchema
	if info.GetSchema() != nil {
		m := newSchemaFromBase(info.GetSchema())
		schema = &m
		schema.Fields = lo.Map(info.GetSchema().GetFields(), func(fieldSchema *schemapb.FieldSchema, _ int) FieldSchema {
			fs := NewFieldSchemaFromBase[*schemapb.FieldSchema, schemapb.DataType](fieldSchema)
			fs.Properties = GetMapFromKVPairs(fieldSchema.GetTypeParams())
			return fs
		})
	}

	return &ChannelWatch{
		Vchan:     getVChannelInfo(info.GetVchan()),
		StartTs:   info.GetStartTs(),
		State:     ChannelWatchState(info.GetState()),
		TimeoutTs: info.GetTimeoutTs(),
		key:       key,
		Schema:    schema,
		Progress:  info.GetProgress(),
		OpID:      info.GetOpID(),

		VchanV2Pb: info.GetVchan(),
	}
}

func getVChannelInfo[info interface {
	vchannelInfoBase
	GetSeekPosition() pos
}, pos msgPosBase](vchan info) VChannelInfo {
	return VChannelInfo{
		CollectionID:        vchan.GetCollectionID(),
		ChannelName:         vchan.GetChannelName(),
		UnflushedSegmentIds: vchan.GetUnflushedSegmentIds(),
		FlushedSegmentIds:   vchan.GetFlushedSegmentIds(),
		DroppedSegmentIds:   vchan.GetDroppedSegmentIds(),
		SeekPosition:        NewMsgPosition(vchan.GetSeekPosition()),
	}
}
