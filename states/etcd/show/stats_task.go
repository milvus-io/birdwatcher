package show

import (
	"context"
	"fmt"

	"github.com/samber/lo"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
)

type StatsTaskParam struct {
	framework.ParamBase `use:"show stats-task" desc:"display stats task information"`
	SubJobType          string `name:"subJobType" default:"" desc:"stats type to filter with, e.g. JsonKeyIndexJob, TextIndexJob, etc."`
	TaskID              string `name:"taskID" default:"" desc:"taskID also known as planID"`
	State               string `name:"state" default:"" desc:"stats state"`
	CollectionID        int64  `name:"collectionID" default:"0" desc:"collection id to filter with"`
	SegmentID           int64  `name:"segmentID" default:"0" desc:"segmentID id to filter with"`
}

func (c *ComponentShow) StatsTaskCommand(ctx context.Context, p *StatsTaskParam) error {
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
			fmt.Printf("  info: %v\n", task.GetProto())
		}
	}
	return nil
}
