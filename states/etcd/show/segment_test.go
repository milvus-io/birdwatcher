package show

import (
	"testing"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
)

func mockSegmentWithDelta(id int64, deltaLogs []*datapb.Binlog) *models.Segment {
	info := &datapb.SegmentInfo{
		ID:           id,
		CollectionID: 1,
		PartitionID:  1,
	}
	return models.NewSegment(info, "", func() ([]*datapb.FieldBinlog, []*datapb.FieldBinlog, []*datapb.FieldBinlog, error) {
		var deltalogs []*datapb.FieldBinlog
		if len(deltaLogs) > 0 {
			deltalogs = []*datapb.FieldBinlog{{Binlogs: deltaLogs}}
		}
		return nil, nil, deltalogs, nil
	})
}

func TestSegmentDeltaStats(t *testing.T) {
	segment := mockSegmentWithDelta(101, []*datapb.Binlog{
		{LogID: 1, LogSize: 1024, MemorySize: 2048, EntriesNum: 10},
		{LogID: 2, LogSize: 1024, MemorySize: 2048, EntriesNum: 20},
	})

	count, logSize, memSize, entriesNum := segmentDeltaStats(segment)
	if count != 2 || logSize != 2048 || memSize != 4096 || entriesNum != 30 {
		t.Errorf("unexpected delta stats: count=%d logSize=%d memSize=%d entriesNum=%d",
			count, logSize, memSize, entriesNum)
	}

	empty := mockSegmentWithDelta(102, nil)
	count, logSize, memSize, entriesNum = segmentDeltaStats(empty)
	if count != 0 || logSize != 0 || memSize != 0 || entriesNum != 0 {
		t.Errorf("expected zero delta stats, got: count=%d logSize=%d memSize=%d entriesNum=%d",
			count, logSize, memSize, entriesNum)
	}
}
