package remove

import (
	"context"
	"fmt"
	"path"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
)

type CompactionTaskParam struct {
	framework.ParamBase `use:"remove compaction" desc:"Remove compaction task"`
	CompactionType      string `name:"type" default:"ClusteringCompaction" desc:"compaction type to remove"`
	JobID               string `name:"jobID" default:"" desc:"jobID also known as triggerID"`
	TaskID              string `name:"taskID" default:"" desc:"taskID also known as planID"`
	Run                 bool   `name:"run" default:"false" desc:"flag to control actually run or dry"`
}

// CompactionTaskCommand is the command function to remove compaction task.
func (c *ComponentRemove) CompactionTaskCommand(ctx context.Context, p *CompactionTaskParam) error {
	compactionTasks, err := common.ListCompactionTask(ctx, c.client, c.basePath, func(task *models.CompactionTask) bool {
		if p.CompactionType != task.GetType().String() {
			return false
		}
		if p.JobID != "" && fmt.Sprint(task.GetTriggerID()) != p.JobID {
			return false
		}
		if p.TaskID != "" && fmt.Sprint(task.GetPlanID()) != p.TaskID {
			return false
		}
		return true
	})
	if err != nil {
		return err
	}

	if len(compactionTasks) == 0 {
		fmt.Println("no compaction task found")
		return nil
	}

	if !p.Run {
		for _, task := range compactionTasks {
			fmt.Printf("target compact task, JobID %d, TaskID %d, Type %s\n", task.GetTriggerID(), task.GetPlanID(), task.GetType().String())
		}
		return nil
	}

	for _, task := range compactionTasks {
		key := path.Join(c.basePath, common.CompactionTaskPrefix, task.GetType().String(), fmt.Sprint(task.GetTriggerID()), fmt.Sprint(task.GetPlanID()))
		err = c.client.RemoveWithPrefix(ctx, key)
		if err != nil {
			return err
		}
		fmt.Printf("clean compaction task done, prefix: %s\n", key)
	}
	return nil
}
