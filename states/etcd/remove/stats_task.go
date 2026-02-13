package remove

import (
	"context"
	"fmt"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
)

type StatsTaskParam struct {
	framework.ExecutionParam `use:"remove stats-task" desc:"Remove stats task"`
	SubJobType               string `name:"subJobType" default:"" desc:"stats type to remove, e.g. JsonKeyIndexJob, TextIndexJob, etc."`
	TaskID                   string `name:"taskID" default:"" desc:"taskID also known as planID"`
	State                    string `name:"state" default:"" desc:"stats state"`
	CollectionID             int64  `name:"collectionID" default:"0" desc:"collection id to filter"`
	SegmentID                int64  `name:"segmentID" default:"0" desc:"segmentID id to filter"`
}

// RemoveStatsTaskCommand is the command function to remove stats task.
func (c *ComponentRemove) RemoveStatsTaskCommand(ctx context.Context, p *StatsTaskParam) error {
	if p.SubJobType == "" {
		return fmt.Errorf("subJobType parameter is required")
	}

	statsTasks, err := common.ListStatsTask(ctx, c.client, c.basePath, func(task *models.StatsTask) bool {
		if p.SubJobType != task.GetProto().GetSubJobType().String() {
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

	if !p.Run {
		for _, task := range statsTasks {
			fmt.Printf("target stats task, TaskID %d, SubJobType %s, State %s, CollectionID %d, SegmentID %d\n",
				task.GetProto().GetTaskID(), task.GetProto().GetSubJobType().String(), task.GetProto().GetState().String(),
				task.GetProto().GetCollectionID(), task.GetProto().GetSegmentID())
		}
		return nil
	}

	for _, task := range statsTasks {
		err = c.client.RemoveWithPrefix(ctx, task.Key())
		if err != nil {
			return err
		}
		fmt.Printf("clean stats task done, prefix: %s\n", task.Key())
	}
	return nil
}
