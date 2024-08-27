package models

import (
	"fmt"
	"sync"

	"github.com/samber/lo"

	"github.com/milvus-io/birdwatcher/proto/v2.0/datapb"
	datapbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/datapb"
)

// Segment is the common model for segment information.
type Segment struct {
	ID                  int64
	CollectionID        int64
	PartitionID         int64
	InsertChannel       string
	NumOfRows           int64
	State               SegmentState
	MaxRowNum           int64
	LastExpireTime      uint64
	CreatedByCompaction bool
	CompactionFrom      []int64
	DroppedAt           uint64
	Level               SegmentLevel
	// position
	StartPosition *MsgPosition
	DmlPosition   *MsgPosition
	// field binlogs
	binlogs   []*FieldBinlog
	statslogs []*FieldBinlog
	deltalogs []*FieldBinlog
	// Semantic version
	Version string

	// PartitionStats version
	PartitionStatsVersion int64

	// sorted by PK
	IsSorted bool

	// etcd segment key
	key string

	// lazy load func
	loadOnce sync.Once
	lazyLoad func(*Segment)
}

func newSegmentFromBase[segmentBase interface {
	GetID() int64
	GetCollectionID() int64
	GetPartitionID() int64
	GetInsertChannel() string
	GetNumOfRows() int64
	GetMaxRowNum() int64
	GetLastExpireTime() uint64
	GetCreatedByCompaction() bool
	GetCompactionFrom() []int64
	GetDroppedAt() uint64
}](info segmentBase) *Segment {
	s := &Segment{}

	s.ID = info.GetID()
	s.CollectionID = info.GetCollectionID()
	s.PartitionID = info.GetPartitionID()
	s.InsertChannel = info.GetInsertChannel()
	s.NumOfRows = info.GetNumOfRows()
	s.MaxRowNum = info.GetMaxRowNum()
	s.LastExpireTime = info.GetLastExpireTime()
	s.CreatedByCompaction = info.GetCreatedByCompaction()
	s.CompactionFrom = info.GetCompactionFrom()
	s.DroppedAt = info.GetDroppedAt()

	return s
}

func NewSegmentFromV2_1(info *datapb.SegmentInfo, key string) *Segment {
	s := newSegmentFromBase(info)
	s.key = key
	s.State = SegmentState(info.GetState())
	s.StartPosition = NewMsgPosition(info.GetStartPosition())
	s.DmlPosition = NewMsgPosition(info.GetDmlPosition())
	s.Level = SegmentLevelLegacy

	mFunc := func(fbl *datapb.FieldBinlog, _ int) *FieldBinlog {
		r := &FieldBinlog{
			FieldID: fbl.GetFieldID(),
			Binlogs: lo.Map(fbl.GetBinlogs(), func(binlog *datapb.Binlog, _ int) *Binlog {
				return newBinlog(binlog)
			}),
		}
		return r
	}
	s.binlogs = lo.Map(info.GetBinlogs(), mFunc)
	s.statslogs = lo.Map(info.GetStatslogs(), mFunc)
	s.deltalogs = lo.Map(info.GetDeltalogs(), mFunc)

	s.Version = "<=2.1.4"
	return s
}

func NewSegmentFromV2_2(info *datapbv2.SegmentInfo, key string,
	lazy func() ([]datapbv2.FieldBinlog, []datapbv2.FieldBinlog, []datapbv2.FieldBinlog, error),
) *Segment {
	s := newSegmentFromBase(info)
	s.key = key
	s.State = SegmentState(info.GetState())
	s.StartPosition = NewMsgPosition(info.GetStartPosition())
	s.DmlPosition = NewMsgPosition(info.GetDmlPosition())
	s.Level = SegmentLevel(info.GetLevel())
	s.IsSorted = info.GetIsSorted()

	s.lazyLoad = func(s *Segment) {
		mFunc := func(fbl datapbv2.FieldBinlog, _ int) *FieldBinlog {
			r := &FieldBinlog{
				FieldID: fbl.GetFieldID(),
				Binlogs: lo.Map(fbl.GetBinlogs(), func(binlog *datapbv2.Binlog, _ int) *Binlog {
					return newBinlogV2(binlog)
				}),
			}
			return r
		}
		binlogs, statslogs, deltalogs, err := lazy()
		if err != nil {
			fmt.Println("lazy load binlog failed", err.Error())
			return
		}
		s.binlogs = lo.Map(binlogs, mFunc)
		s.statslogs = lo.Map(statslogs, mFunc)
		s.deltalogs = lo.Map(deltalogs, mFunc)
	}

	s.Version = ">=2.2.0"
	s.PartitionStatsVersion = info.GetPartitionStatsVersion()
	return s
}

func (s *Segment) GetBinlogs() []*FieldBinlog {
	s.loadOnce.Do(func() {
		if s.lazyLoad != nil {
			s.lazyLoad(s)
		}
	})
	return s.binlogs
}

func (s *Segment) GetStatslogs() []*FieldBinlog {
	s.loadOnce.Do(func() {
		if s.lazyLoad != nil {
			s.lazyLoad(s)
		}
	})
	return s.statslogs
}

func (s *Segment) GetDeltalogs() []*FieldBinlog {
	s.loadOnce.Do(func() {
		if s.lazyLoad != nil {
			s.lazyLoad(s)
		}
	})
	return s.deltalogs
}

func (s *Segment) GetStartPosition() *MsgPosition {
	if s == nil {
		return nil
	}
	return s.StartPosition
}

func (s *Segment) GetDmlPosition() *MsgPosition {
	if s == nil {
		return nil
	}
	return s.DmlPosition
}

type MsgPosition struct {
	ChannelName string
	MsgID       []byte
	MsgGroup    string
	Timestamp   uint64
}

type msgPosBase interface {
	GetChannelName() string
	GetMsgID() []byte
	GetMsgGroup() string
	GetTimestamp() uint64
}

func NewMsgPosition[T msgPosBase](pos T) *MsgPosition {
	return &MsgPosition{
		ChannelName: pos.GetChannelName(),
		MsgID:       pos.GetMsgID(),
		MsgGroup:    pos.GetMsgGroup(),
		Timestamp:   pos.GetTimestamp(),
	}
}

func (pos *MsgPosition) GetTimestamp() uint64 {
	if pos == nil {
		return 0
	}
	return pos.Timestamp
}

func (pos *MsgPosition) GetChannelName() string {
	if pos == nil {
		return ""
	}
	return pos.ChannelName
}

type FieldBinlog struct {
	FieldID int64
	Binlogs []*Binlog
}

type Binlog struct {
	EntriesNum    int64
	TimestampFrom uint64
	TimestampTo   uint64
	LogPath       string
	LogSize       int64
	LogID         int64
	MemSize       int64
}

func newBinlog[T interface {
	GetEntriesNum() int64
	GetTimestampFrom() uint64
	GetTimestampTo() uint64
	GetLogPath() string
	GetLogSize() int64
}](binlog T) *Binlog {
	return &Binlog{
		EntriesNum:    binlog.GetEntriesNum(),
		TimestampFrom: binlog.GetTimestampFrom(),
		TimestampTo:   binlog.GetTimestampTo(),
		LogPath:       binlog.GetLogPath(),
		LogSize:       binlog.GetLogSize(),
	}
}

func newBinlogV2[T interface {
	GetEntriesNum() int64
	GetTimestampFrom() uint64
	GetTimestampTo() uint64
	GetLogPath() string
	GetLogSize() int64
	GetLogID() int64
	GetMemorySize() int64
}](binlog T) *Binlog {
	return &Binlog{
		EntriesNum:    binlog.GetEntriesNum(),
		TimestampFrom: binlog.GetTimestampFrom(),
		TimestampTo:   binlog.GetTimestampTo(),
		LogPath:       binlog.GetLogPath(),
		LogSize:       binlog.GetLogSize(),
		LogID:         binlog.GetLogID(),
		MemSize:       binlog.GetMemorySize(),
	}
}
