package show

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/samber/lo"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/birdwatcher/utils"
)

type StatsTaskParam struct {
	framework.DataSetParam `use:"show stats-task" desc:"display stats task information"`
	SubJobType             string `name:"subJobType" default:"" desc:"stats type to filter with, e.g. JsonKeyIndexJob, TextIndexJob, etc."`
	TaskID                 string `name:"taskID" default:"" desc:"taskID also known as planID"`
	State                  string `name:"state" default:"" desc:"stats state"`
	CollectionID           int64  `name:"collectionID" default:"0" desc:"collection id to filter with"`
	SegmentID              int64  `name:"segmentID" default:"0" desc:"segmentID id to filter with"`
	Since                  string `name:"since" default:"" desc:"only show tasks created after this time, format: 2006-01-02 15:04:05 or duration like 1h, 30m"`
	Failed                 bool   `name:"failed" default:"false" desc:"only show tasks with fail reason"`
}

func (c *ComponentShow) StatsTaskCommand(ctx context.Context, p *StatsTaskParam) (*framework.PresetResultSet, error) {
	var sinceTime time.Time
	if p.Since != "" {
		// try parse as duration first (e.g., "1h", "30m")
		if duration, err := time.ParseDuration(p.Since); err == nil {
			sinceTime = time.Now().Add(-duration)
		} else if t, err := time.ParseInLocation("2006-01-02 15:04:05", p.Since, time.Local); err == nil {
			sinceTime = t
		} else {
			return nil, fmt.Errorf("invalid since format: %s, use format like '2006-01-02 15:04:05' or duration like '1h', '30m'", p.Since)
		}
	}

	statsTasks, err := common.ListStatsTask(ctx, c.client, c.metaPath, func(task *models.StatsTask) bool {
		if p.SubJobType != "" && p.SubJobType != task.GetProto().GetSubJobType().String() {
			return false
		}
		if p.TaskID != "" && fmt.Sprint(task.GetProto().GetTaskID()) != p.TaskID {
			return false
		}
		if p.State != "" && task.GetProto().GetState().String() != p.State {
			return false
		}
		if p.CollectionID != 0 && task.GetProto().GetCollectionID() != p.CollectionID {
			return false
		}
		if p.SegmentID != 0 && task.GetProto().GetSegmentID() != p.SegmentID {
			return false
		}
		if !sinceTime.IsZero() {
			createTime, _ := utils.ParseTS(uint64(task.GetProto().GetTaskID()))
			if createTime.Before(sinceTime) {
				return false
			}
		}
		if p.Failed && task.GetProto().GetFailReason() == "" {
			return false
		}
		return true
	})
	if err != nil {
		return nil, err
	}

	rs := &StatsTasks{tasks: statsTasks}
	return framework.NewPresetResultSet(rs, framework.NameFormat(p.Format)), nil
}

type StatsTasks struct {
	tasks []*models.StatsTask
}

func (rs *StatsTasks) Entities() any {
	return rs.tasks
}

func (rs *StatsTasks) PrintAs(format framework.Format) string {
	switch format {
	case framework.FormatDefault, framework.FormatPlain:
		return rs.printDefault()
	case framework.FormatJSON:
		return rs.printAsJSON()
	}
	return ""
}

func (rs *StatsTasks) printDefault() string {
	sb := &strings.Builder{}

	if len(rs.tasks) == 0 {
		fmt.Fprintln(sb, "no stats task found")
		return sb.String()
	}

	task2Col := lo.GroupBy(rs.tasks, func(task *models.StatsTask) int64 {
		return task.GetProto().GetCollectionID()
	})

	for colID, tasks := range task2Col {
		fmt.Fprintf(sb, "CollectionID: %d, Tasks: %d\n", colID, len(tasks))
		for _, task := range tasks {
			proto := task.GetProto()
			createTime, _ := utils.ParseTS(uint64(proto.GetTaskID()))
			fmt.Fprintf(sb, "  TaskID: %d, SegmentID: %d, Type: %s, State: %s, CanRecycle: %v, CreateTime: %s, FailReason: %s\n",
				proto.GetTaskID(),
				proto.GetSegmentID(),
				proto.GetSubJobType().String(),
				proto.GetState().String(),
				proto.GetCanRecycle(),
				createTime.Format("2006-01-02 15:04:05"),
				proto.GetFailReason())
		}
	}
	return sb.String()
}

func (rs *StatsTasks) printAsJSON() string {
	type StatsTaskJSON struct {
		TaskID       int64  `json:"task_id"`
		CollectionID int64  `json:"collection_id"`
		SegmentID    int64  `json:"segment_id"`
		SubJobType   string `json:"sub_job_type"`
		State        string `json:"state"`
		CanRecycle   bool   `json:"can_recycle"`
		CreateTime   string `json:"create_time"`
		FailReason   string `json:"fail_reason,omitempty"`
	}

	type OutputJSON struct {
		Tasks []StatsTaskJSON `json:"tasks"`
		Total int             `json:"total"`
	}

	output := OutputJSON{
		Tasks: make([]StatsTaskJSON, 0, len(rs.tasks)),
		Total: len(rs.tasks),
	}

	for _, task := range rs.tasks {
		proto := task.GetProto()
		createTime, _ := utils.ParseTS(uint64(proto.GetTaskID()))
		output.Tasks = append(output.Tasks, StatsTaskJSON{
			TaskID:       proto.GetTaskID(),
			CollectionID: proto.GetCollectionID(),
			SegmentID:    proto.GetSegmentID(),
			SubJobType:   proto.GetSubJobType().String(),
			State:        proto.GetState().String(),
			CanRecycle:   proto.GetCanRecycle(),
			CreateTime:   createTime.Format("2006-01-02 15:04:05"),
			FailReason:   proto.GetFailReason(),
		})
	}

	return framework.MarshalJSON(output)
}
