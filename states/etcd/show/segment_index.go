package show

import (
	"context"
	"fmt"
	"strings"

	"github.com/samber/lo"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
)

type SegmentIndexParam struct {
	framework.DataSetParam `use:"show segment-index" desc:"display segment index information" alias:"segments-index,segment-indexes,segments-indexes"`
	CollectionID           int64 `name:"collection" default:"0" desc:"collection id to filter with"`
	SegmentID              int64 `name:"segment" default:"0" desc:"segment id to filter with"`
	FieldID                int64 `name:"field" default:"0" desc:"field id to filter with"`
	IndexID                int64 `name:"indexID" default:"0" desc:"index id to filter with"`
}

// SegmentIndexCommand returns show segment-index command.
func (c *ComponentShow) SegmentIndexCommand(ctx context.Context, p *SegmentIndexParam) (*framework.PresetResultSet, error) {
	segments, err := common.ListSegments(ctx, c.client, c.metaPath, func(info *models.Segment) bool {
		return (p.CollectionID == 0 || info.CollectionID == p.CollectionID) &&
			(p.SegmentID == 0 || info.ID == p.SegmentID)
	})
	if err != nil {
		return nil, err
	}

	segmentIndexes, err := common.ListSegmentIndex(ctx, c.client, c.metaPath, func(segIdx *models.SegmentIndex) bool {
		return (p.CollectionID == 0 || p.CollectionID == segIdx.GetProto().GetCollectionID()) &&
			(p.SegmentID == 0 || p.SegmentID == segIdx.GetProto().GetSegmentID()) &&
			(p.IndexID == 0 || p.IndexID == segIdx.GetProto().GetIndexID())
	})
	if err != nil {
		return nil, err
	}

	indexBuildInfo, err := common.ListIndex(ctx, c.client, c.metaPath, func(index *models.FieldIndex) bool {
		return p.CollectionID == 0 || p.CollectionID == index.GetProto().GetIndexInfo().GetCollectionID() &&
			(p.SegmentID == 0 || p.SegmentID == index.GetProto().GetIndexInfo().GetFieldID()) &&
			(p.FieldID == 0 || p.FieldID == index.GetProto().GetIndexInfo().GetFieldID()) &&
			(p.IndexID == 0 || p.IndexID == index.GetProto().GetIndexInfo().GetIndexID())
	})
	if err != nil {
		return nil, err
	}

	rs := &SegmentIndexes{
		segments:       segments,
		segmentIndexes: segmentIndexes,
		indexBuildInfo: indexBuildInfo,
	}

	return framework.NewPresetResultSet(rs, framework.NameFormat(p.Format)), nil
}

type SegmentIndexes struct {
	segments       []*models.Segment
	segmentIndexes []*models.SegmentIndex
	indexBuildInfo []*models.FieldIndex
}

func (rs *SegmentIndexes) Entities() any {
	return rs.segmentIndexes
}

func (rs *SegmentIndexes) PrintAs(format framework.Format) string {
	switch format {
	case framework.FormatDefault, framework.FormatPlain:
		return rs.printDefault()
	case framework.FormatJSON:
		return rs.printAsJSON()
	}
	return ""
}

func (rs *SegmentIndexes) printDefault() string {
	sb := &strings.Builder{}

	seg2Idx := lo.GroupBy(rs.segmentIndexes, func(segIdx *models.SegmentIndex) int64 {
		return segIdx.GetProto().GetSegmentID()
	})

	idIdx := lo.SliceToMap(rs.indexBuildInfo, func(info *models.FieldIndex) (int64, *models.FieldIndex) {
		return info.GetProto().GetIndexInfo().GetIndexID(), info
	})

	count := make(map[string]int)
	var totalSize uint64

	for _, segment := range rs.segments {
		if segment.State != commonpb.SegmentState_Flushed && segment.GetState() != commonpb.SegmentState_Flushing {
			continue
		}
		fmt.Fprintf(sb, "SegmentID: %d\t State: %s\n", segment.GetID(), segment.GetState().String())
		segIdxs, ok := seg2Idx[segment.GetID()]
		if !ok {
			continue
		}
		for _, info := range segIdxs {
			segIdx := info.GetProto()
			index, ok := idIdx[segIdx.GetIndexID()]
			if !ok {
				continue
			}
			fmt.Fprintf(sb, "\tIndexID: %d\tIndex build ID: %d, states %s", segIdx.GetIndexID(), segIdx.GetBuildID(), segIdx.GetState().String())
			count[segIdx.GetState().String()]++

			fmt.Fprintf(sb, "\t Index Type:%v on Field ID: %d", common.GetKVPair(index.GetProto().GetIndexInfo().GetIndexParams(), "index_type"), index.GetProto().GetIndexInfo().GetFieldID())
			fmt.Fprintf(sb, "\tSerialized Size: %d\n", segIdx.GetSerializeSize())
			fmt.Fprintf(sb, "\tCurrent Index Version: %d\n", segIdx.GetCurrentIndexVersion())
			fmt.Fprintf(sb, "\t Index Files: %v\n", segIdx.IndexFileKeys)

			totalSize += segIdx.GetSerializeSize()
		}
		fmt.Fprintln(sb)
	}

	for idxSta, cnt := range count {
		fmt.Fprintf(sb, "[%s]: %d\t", idxSta, cnt)
	}
	fmt.Fprintln(sb, "Total Serialized size:", totalSize)
	fmt.Fprintln(sb)

	return sb.String()
}

func (rs *SegmentIndexes) printAsJSON() string {
	type SegmentIndexJSON struct {
		SegmentID           int64    `json:"segment_id"`
		IndexID             int64    `json:"index_id"`
		BuildID             int64    `json:"build_id"`
		State               string   `json:"state"`
		IndexType           string   `json:"index_type,omitempty"`
		FieldID             int64    `json:"field_id,omitempty"`
		SerializedSize      uint64   `json:"serialized_size"`
		CurrentIndexVersion int32    `json:"current_index_version"`
		IndexFileKeys       []string `json:"index_file_keys,omitempty"`
	}

	type OutputJSON struct {
		SegmentIndexes      []SegmentIndexJSON `json:"segment_indexes"`
		StateCount          map[string]int     `json:"state_count"`
		TotalSerializedSize uint64             `json:"total_serialized_size"`
	}

	seg2Idx := lo.GroupBy(rs.segmentIndexes, func(segIdx *models.SegmentIndex) int64 {
		return segIdx.GetProto().GetSegmentID()
	})

	idIdx := lo.SliceToMap(rs.indexBuildInfo, func(info *models.FieldIndex) (int64, *models.FieldIndex) {
		return info.GetProto().GetIndexInfo().GetIndexID(), info
	})

	output := OutputJSON{
		SegmentIndexes: make([]SegmentIndexJSON, 0),
		StateCount:     make(map[string]int),
	}

	for _, segment := range rs.segments {
		if segment.State != commonpb.SegmentState_Flushed && segment.GetState() != commonpb.SegmentState_Flushing {
			continue
		}
		segIdxs, ok := seg2Idx[segment.GetID()]
		if !ok {
			continue
		}
		for _, info := range segIdxs {
			segIdx := info.GetProto()
			index, ok := idIdx[segIdx.GetIndexID()]

			item := SegmentIndexJSON{
				SegmentID:           segment.GetID(),
				IndexID:             segIdx.GetIndexID(),
				BuildID:             segIdx.GetBuildID(),
				State:               segIdx.GetState().String(),
				SerializedSize:      segIdx.GetSerializeSize(),
				CurrentIndexVersion: segIdx.GetCurrentIndexVersion(),
				IndexFileKeys:       segIdx.IndexFileKeys,
			}

			if ok {
				item.IndexType = common.GetKVPair(index.GetProto().GetIndexInfo().GetIndexParams(), "index_type")
				item.FieldID = index.GetProto().GetIndexInfo().GetFieldID()
			}

			output.SegmentIndexes = append(output.SegmentIndexes, item)
			output.StateCount[segIdx.GetState().String()]++
			output.TotalSerializedSize += segIdx.GetSerializeSize()
		}
	}

	return framework.MarshalJSON(output)
}
