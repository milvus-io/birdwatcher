package show

import (
	"context"
	"fmt"
	"time"

	"github.com/samber/lo"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/birdwatcher/utils"
)

type StatsTaskParam struct {
	framework.ParamBase `use:"show stats-task" desc:"display stats task information"`
	SubJobType          string `name:"subJobType" default:"" desc:"stats type to filter with, e.g. JsonKeyIndexJob, TextIndexJob, etc."`
	TaskID              string `name:"taskID" default:"" desc:"taskID also known as planID"`
	State               string `name:"state" default:"" desc:"stats state"`
	CollectionID        int64  `name:"collectionID" default:"0" desc:"collection id to filter with"`
	SegmentID           int64  `name:"segmentID" default:"0" desc:"segmentID id to filter with"`
	Since               string `name:"since" default:"" desc:"only show tasks created after this time, format: 2006-01-02 15:04:05 or duration like 1h, 30m"`
	Failed              bool   `name:"failed" default:"false" desc:"only show tasks with fail reason"`
}

func (c *ComponentShow) StatsTaskCommand(ctx context.Context, p *StatsTaskParam) error {
	var sinceTime time.Time
	if p.Since != "" {
		// try parse as duration first (e.g., "1h", "30m")
		if duration, err := time.ParseDuration(p.Since); err == nil {
			sinceTime = time.Now().Add(-duration)
		} else if t, err := time.ParseInLocation("2006-01-02 15:04:05", p.Since, time.Local); err == nil {
			sinceTime = t
		} else {
			return fmt.Errorf("invalid since format: %s, use format like '2006-01-02 15:04:05' or duration like '1h', '30m'", p.Since)
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
		return err
	}

	if len(statsTasks) == 0 {
		fmt.Println("no stats task found")
		return nil
	}

	task2Col := lo.GroupBy(statsTasks, func(task *models.StatsTask) int64 {
		return task.GetProto().GetCollectionID()
	})

	for colID, tasks := range task2Col {
		fmt.Printf("CollectionID: %d, Tasks: %d\n", colID, len(tasks))
		for _, task := range tasks {
			proto := task.GetProto()
			createTime, _ := utils.ParseTS(uint64(proto.GetTaskID()))
			fmt.Printf("  TaskID: %d, SegmentID: %d, Type: %s, State: %s, CanRecycle: %v, CreateTime: %s, FailReason: %s\n",
				proto.GetTaskID(),
				proto.GetSegmentID(),
				proto.GetSubJobType().String(),
				proto.GetState().String(),
				proto.GetCanRecycle(),
				createTime.Format("2006-01-02 15:04:05"),
				proto.GetFailReason())
		}
	}
	return nil
}
