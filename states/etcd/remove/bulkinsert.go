package remove

import (
	"context"
	"fmt"
	"time"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/birdwatcher/states/etcd/show"
)

type RemoveImportJobParam struct {
	framework.ExecutionParam `use:"remove import-job" desc:"Remove import job from datacoord meta with specified job id" alias:"import"`

	JobID int64 `name:"job" default:"" desc:"import job id to remove"`
}

type RemoveImportTaskParam struct {
	framework.ExecutionParam `use:"remove import-task" desc:"Remove import task from datacoord meta with specified task id"`

	TaskID int64 `name:"task" default:"0" desc:"import task id to remove"`
}

func (c *ComponentRemove) RemoveImportJobCommand(ctx context.Context, p *RemoveImportJobParam) error {
	jobs, err := common.ListImportJobs(ctx, c.client, c.basePath, func(job *models.ImportJob) bool {
		return job.GetProto().GetJobID() == p.JobID
	})
	if err != nil {
		fmt.Println("failed to list bulkinsert jobs, err=", err.Error())
		return err
	}

	if len(jobs) == 0 {
		fmt.Printf("cannot find target import job: %d\n", p.JobID)
		return nil
	}
	if len(jobs) > 1 {
		fmt.Printf("unexpected import job, expect 1, but got %d\n", len(jobs))
		return nil
	}

	targetJob := jobs[0].GetProto()
	targetPath := jobs[0].Key()
	fmt.Printf("selected target import job, jobID: %d, key=%s\n", targetJob.GetJobID(), targetPath)
	show.PrintDetailedImportJob(ctx, c.client, c.basePath, targetJob, false)

	if !p.Run {
		return nil
	}

	fmt.Printf("Start to delete import job...\n")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	err = c.client.Remove(ctx, targetPath)
	cancel()
	if err != nil {
		fmt.Printf("failed to delete import job %d, error: %s\n", targetJob.GetJobID(), err.Error())
		return err
	}
	fmt.Printf("remove import job %d done\n", targetJob.GetJobID())
	return nil
}

func (c *ComponentRemove) RemoveImportTaskCommand(ctx context.Context, p *RemoveImportTaskParam) error {
	if p.TaskID == 0 {
		fmt.Println("Error: task id is required (use --task <taskID>)")
		return nil
	}

	// Search in PreImportTasks
	preimportTasks, err := common.ListPreImportTasks(ctx, c.client, c.basePath, func(task *models.PreImportTask) bool {
		return task.GetProto().GetTaskID() == p.TaskID
	})
	if err != nil {
		fmt.Println("failed to list preimport tasks, err=", err.Error())
		return err
	}

	// Search in ImportTaskV2
	importTasks, err := common.ListImportTasks(ctx, c.client, c.basePath, func(task *models.ImportTaskV2) bool {
		return task.GetProto().GetTaskID() == p.TaskID
	})
	if err != nil {
		fmt.Println("failed to list import tasks, err=", err.Error())
		return err
	}

	// Handle not found
	if len(preimportTasks) == 0 && len(importTasks) == 0 {
		fmt.Printf("cannot find target import task: %d\n", p.TaskID)
		return nil
	}

	// Show what will be deleted
	if len(preimportTasks) > 0 {
		if len(importTasks) > 0 {
			fmt.Println("Warning: Found task in both PreImportTask and ImportTaskV2, removing both")
		}
		for _, task := range preimportTasks {
			fmt.Printf("Selected PreImportTask, taskID: %d, key=%s\n", task.GetProto().GetTaskID(), task.Key())
			show.PrintPreImportTask(task.GetProto(), false)
		}
	}

	if len(importTasks) > 0 {
		for _, task := range importTasks {
			fmt.Printf("Selected ImportTaskV2, taskID: %d, key=%s\n", task.GetProto().GetTaskID(), task.Key())
			show.PrintImportTask(task.GetProto(), false)
		}
	}

	if !p.Run {
		return nil
	}

	// Delete PreImportTasks
	fmt.Printf("Start to delete import task(s)...\n")
	for _, task := range preimportTasks {
		delCtx, cancel := context.WithTimeout(ctx, time.Second*3)
		err = c.client.Remove(delCtx, task.Key())
		cancel()
		if err != nil {
			fmt.Printf("failed to delete PreImportTask %d, error: %s\n", task.GetProto().GetTaskID(), err.Error())
			return err
		}
		fmt.Printf("removed PreImportTask %d done\n", task.GetProto().GetTaskID())
	}

	// Delete ImportTaskV2
	for _, task := range importTasks {
		delCtx, cancel := context.WithTimeout(ctx, time.Second*3)
		err = c.client.Remove(delCtx, task.Key())
		cancel()
		if err != nil {
			fmt.Printf("failed to delete ImportTaskV2 %d, error: %s\n", task.GetProto().GetTaskID(), err.Error())
			return err
		}
		fmt.Printf("removed ImportTaskV2 %d done\n", task.GetProto().GetTaskID())
	}

	return nil
}
