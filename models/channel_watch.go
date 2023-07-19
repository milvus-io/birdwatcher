package models

type ChannelWatch struct {
	Vchan     VChannelInfo
	StartTs   int64
	State     ChannelWatchState
	TimeoutTs int64

	// key
	key string
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
