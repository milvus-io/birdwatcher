package show

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/samber/lo"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
)

// CompactionCommand returns sub command for showCmd.
// show compaction [options...]
type CompactionTaskParam struct {
	framework.ParamBase `use:"show compactions" desc:"list current available compactions from DataCoord"`
	CollectionName      string `name:"collectionName" default:"" desc:"collection name to display"`
	State               string `name:"state" default:"" desc:"compaction state to filter"`
	CollectionID        int64  `name:"collectionID" default:"0" desc:"collection id to filter"`
	PartitionID         int64  `name:"partitionID" default:"0" desc:"partitionID id to filter"`
	CompactionType      string `name:"type" default:"" desc:"compaction type to filter"`
	TriggerID           int64  `name:"triggerID" default:"0" desc:"TriggerID to filter"`
	PlanID              int64  ` name:"planID" default:"0" desc:"PlanID  to filter"`
	SegmentID           int64  ` name:"segmentID" default:"0" desc:"SegmentID  to filter"`
	Detail              bool   `name:"detail" default:"false" desc:"flags indicating whether printing input/result segmentIDs"`
	IgnoreDone          bool   `name:"ignoreDone" default:"true" desc:"ignore finished compaction tasks"`
	Format              string `name:"format" default:"" desc:"output format (default, json)"`
}

func (c *ComponentShow) CompactionTaskCommand(ctx context.Context, p *CompactionTaskParam) (*framework.PresetResultSet, error) {
	var compactionTasks []*models.CompactionTask
	var err error
	var total int64

	// perform get by id to accelerate

	compactionTasks, err = common.ListCompactionTask(ctx, c.client, c.metaPath, func(task *models.CompactionTask) bool {
		total++
		if p.CollectionName != "" && task.GetSchema().GetName() != p.CollectionName {
			return false
		}
		if p.CompactionType != "" && p.CompactionType != task.GetType().String() {
			return false
		}
		if p.CollectionID > 0 && task.GetCollectionID() != p.CollectionID {
			return false
		}
		if p.PartitionID > 0 && task.GetPartitionID() != p.PartitionID {
			return false
		}
		if p.TriggerID > 0 && task.GetTriggerID() != p.TriggerID {
			return false
		}
		if p.PlanID > 0 && task.GetPlanID() != p.PlanID {
			return false
		}

		if p.SegmentID > 0 && !lo.Contains(task.GetInputSegments(), p.SegmentID) {
			return false
		}
		if p.IgnoreDone &&
			(strings.EqualFold(task.GetState().String(), "cleaned") || strings.EqualFold(task.GetState().String(), "completed")) {
			return false
		}
		if p.State != "" && !strings.EqualFold(p.State, task.GetState().String()) {
			return false
		}
		return true
	})
	if err != nil {
		return nil, err
	}
	if p.IgnoreDone {
		fmt.Println("ignoreDone flag set to true, set `--ignoreDone=false` to show all tasks")
	}
	sort.Slice(compactionTasks, func(i, j int) bool {
		return compactionTasks[i].GetPlanID() < compactionTasks[j].GetPlanID()
	})
	result := &CompactionTasks{
		tasks: compactionTasks,
		total: total,
		param: p,
	}
	return framework.NewPresetResultSet(result, framework.NameFormat(p.Format)), nil
}

type CompactionTasks struct {
	tasks []*models.CompactionTask
	total int64
	param *CompactionTaskParam
}

func (rs *CompactionTasks) PrintAs(format framework.Format) string {
	switch format {
	case framework.FormatDefault, framework.FormatPlain:
		sb := &strings.Builder{}
		for _, t := range rs.tasks {
			if rs.param.Detail {
				printCompactionTask(sb, t, rs.param.Detail)
			} else {
				printCompactionTaskSimple(sb, t)
			}
		}
		fmt.Fprintln(sb, "================================================================================")
		fmt.Fprintf(sb, "--- Total compactions:  %d\t Matched compactions:  %d\n", rs.total, len(rs.tasks))
		return sb.String()
	case framework.FormatJSON:
		return rs.printAsJSON()
	}
	return ""
}

func (rs *CompactionTasks) printAsJSON() string {
	type CompactionTaskJSON struct {
		PlanID         int64   `json:"plan_id"`
		TriggerID      int64   `json:"trigger_id"`
		CollectionID   int64   `json:"collection_id"`
		CollectionName string  `json:"collection_name"`
		PartitionID    int64   `json:"partition_id,omitempty"`
		Channel        string  `json:"channel"`
		Type           string  `json:"type"`
		State          string  `json:"state"`
		StartTime      string  `json:"start_time"`
		EndTime        string  `json:"end_time,omitempty"`
		NodeID         int64   `json:"node_id,omitempty"`
		TotalRows      int64   `json:"total_rows,omitempty"`
		InputSegments  []int64 `json:"input_segments,omitempty"`
		ResultSegments []int64 `json:"result_segments,omitempty"`
	}

	type OutputJSON struct {
		Compactions  []CompactionTaskJSON `json:"compactions"`
		TotalCount   int64                `json:"total_count"`
		MatchedCount int                  `json:"matched_count"`
	}

	output := OutputJSON{
		Compactions:  make([]CompactionTaskJSON, 0, len(rs.tasks)),
		TotalCount:   rs.total,
		MatchedCount: len(rs.tasks),
	}

	for _, t := range rs.tasks {
		task := CompactionTaskJSON{
			PlanID:         t.GetPlanID(),
			TriggerID:      t.GetTriggerID(),
			CollectionID:   t.GetCollectionID(),
			CollectionName: t.GetSchema().GetName(),
			PartitionID:    t.GetPartitionID(),
			Channel:        t.GetChannel(),
			Type:           t.GetType().String(),
			State:          t.GetState().String(),
			NodeID:         t.GetNodeID(),
			TotalRows:      t.GetTotalRows(),
		}

		startT := time.Unix(t.GetStartTime(), 0)
		task.StartTime = startT.Format("2006-01-02 15:04:05")

		if t.GetEndTime() > 0 {
			endT := time.Unix(t.GetEndTime(), 0)
			task.EndTime = endT.Format("2006-01-02 15:04:05")
		}

		if rs.param.Detail {
			task.InputSegments = t.GetInputSegments()
			task.ResultSegments = t.GetResultSegments()
		}

		output.Compactions = append(output.Compactions, task)
	}

	return framework.MarshalJSON(output)
}

func (rs *CompactionTasks) Entities() any {
	return rs.tasks
}

func printCompactionTaskSimple(sb *strings.Builder, task *models.CompactionTask) {
	fmt.Fprintf(sb, "JobID: %d\tTaskID: %d\t Type:%s\t State:%s\t StartTime: %d\n", task.GetTriggerID(), task.GetPlanID(), task.GetType().String(), task.GetState().String(), task.GetStartTime())
}

func printCompactionTask(sb *strings.Builder, task *models.CompactionTask, detailSegmentIDs bool) {
	fmt.Fprintln(sb, "================================================================================")
	if task.GetPartitionID() != 0 {
		fmt.Fprintf(sb, "Collection ID: %d\tCollection Name: %s\t PartitionID:%d\t Channel:%s\t StartTime: %d\n", task.GetCollectionID(), task.GetSchema().GetName(), task.GetPartitionID(), task.GetChannel(), task.GetStartTime())
	} else {
		fmt.Fprintf(sb, "Collection ID: %d\tCollection Name: %s\t Channel:%s\t StartTime: %d\n", task.GetCollectionID(), task.GetSchema().GetName(), task.GetChannel(), task.GetStartTime())
	}
	fmt.Fprintf(sb, "JobID: %d\tTaskID: %d\t Type:%s\t State:%s\t StartTime: %d\n", task.GetTriggerID(), task.GetPlanID(), task.GetType().String(), task.GetState().String(), task.GetStartTime())

	t := time.Unix(task.GetStartTime(), 0)
	fmt.Fprintf(sb, "Start Time: %s\n", t.Format("2006-01-02 15:04:05"))
	if task.GetEndTime() > 0 {
		endT := time.Unix(task.GetEndTime(), 0)
		fmt.Fprintf(sb, "End Time: %s\n", endT.Format("2006-01-02 15:04:05"))
	}

	if task.GetClusteringKeyField() != nil {
		fmt.Fprintf(sb, "ClusterField Name:%s\t DataType:%s\n", task.GetClusteringKeyField().GetName(), task.GetClusteringKeyField().GetDataType().String())
	}

	if task.GetNodeID() > 0 {
		fmt.Fprintf(sb, "WorkerID :%d\n", task.GetNodeID())
	}
	if task.GetTotalRows() > 0 {
		fmt.Fprintf(sb, "Total Rows :%d\n", task.GetTotalRows())
	}

	if detailSegmentIDs {
		fmt.Fprintf(sb, "Input Segments:%v\n", task.GetInputSegments())
		fmt.Fprintf(sb, "Target Segments:%v\n", task.GetResultSegments())
	}
}
