package show

import (
	"strings"
	"testing"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
)

func mockSegmentWithDelta(id, collectionID int64, deltaLogs []*datapb.Binlog) *models.Segment {
	info := &datapb.SegmentInfo{
		ID:           id,
		CollectionID: collectionID,
		PartitionID:  1,
		State:        commonpb.SegmentState_Flushed,
		NumOfRows:    100,
		MaxRowNum:    1000,
	}
	return models.NewSegment(info, "", func() ([]*datapb.FieldBinlog, []*datapb.FieldBinlog, []*datapb.FieldBinlog, []*datapb.FieldBinlog, error) {
		var deltalogs []*datapb.FieldBinlog
		if len(deltaLogs) > 0 {
			deltalogs = []*datapb.FieldBinlog{{Binlogs: deltaLogs}}
		}
		return nil, nil, deltalogs, nil, nil
	})
}

func TestSegmentsDeltaFormat(t *testing.T) {
	segments := []*models.Segment{
		mockSegmentWithDelta(101, 1, []*datapb.Binlog{
			{LogID: 1, LogSize: 1024, MemorySize: 2048, EntriesNum: 10},
			{LogID: 2, LogSize: 1024, MemorySize: 2048, EntriesNum: 20},
		}),
		mockSegmentWithDelta(102, 1, nil),
	}

	rs := &Segments{segments: segments, format: "delta"}
	output := rs.PrintAs(framework.FormatDefault)

	for _, want := range []string{
		"SegmentID: 101",
		"DeltaLog Count: 2",
		"Delta Entries: 30",
		"SegmentID: 102",
		"DeltaLog Count: 0",
		"--- Collection deltalog count: 2",
		"--- Total deltalog count: 2",
	} {
		if !strings.Contains(output, want) {
			t.Errorf("delta format output missing %q, got:\n%s", want, output)
		}
	}
}

func TestSegmentsJSONDeltaFields(t *testing.T) {
	segments := []*models.Segment{
		mockSegmentWithDelta(101, 1, []*datapb.Binlog{
			{LogID: 1, LogSize: 1024, MemorySize: 2048, EntriesNum: 10},
		}),
	}

	rs := &Segments{segments: segments}
	output := rs.PrintAs(framework.FormatJSON)

	for _, want := range []string{
		`"delta_log_count": 1`,
		`"delta_log_size": 1024`,
		`"delta_mem_size": 2048`,
		`"delta_entries_num": 10`,
		`"total_delta_log_count": 1`,
		`"total_delta_log_size": 1024`,
		`"total_delta_entries_num": 10`,
	} {
		if !strings.Contains(output, want) {
			t.Errorf("JSON output missing %q, got:\n%s", want, output)
		}
	}
}
