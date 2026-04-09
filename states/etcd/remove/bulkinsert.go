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

	JobID        int64 `name:"job" default:"" desc:"import job id to remove"`
	WithoutTasks bool  `name:"without-tasks" default:"false" desc:"remove job only, keep associated tasks"`
}

type RemoveImportTaskParam struct {
	framework.ExecutionParam `use:"remove import-task" desc:"Remove import task from datacoord meta with specified task id"`

	TaskID int64 `name:"task" default:"0" desc:"import task id to remove"`
}

func (c *ComponentRemove) RemoveImportJobCommand(ctx context.Context, p *RemoveImportJobParam) error {
	jobs, err := common.ListImportJobs(ctx, c.client, c.metaPath, func(job *models.ImportJob) bool {
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
	show.PrintDetailedImportJob(ctx, c.client, c.metaPath, targetJob, false)

	// If without-tasks flag is set, remove only the job (old behavior)
	if p.WithoutTasks {
		if !p.Run {
			return nil
		}

		fmt.Printf("Start to delete import job (without tasks)...\n")
		delCtx, cancel := context.WithTimeout(ctx, time.Second*3)
		err = c.client.Remove(delCtx, targetPath)
		cancel()
		if err != nil {
			fmt.Printf("failed to delete import job %d, error: %s\n", targetJob.GetJobID(), err.Error())
			return err
		}
		fmt.Printf("remove import job %d done\n", targetJob.GetJobID())
		return nil
	}

	// Default behavior: remove job with associated tasks
	// Query associated PreImportTasks
	preimportTasks, err := common.ListPreImportTasks(ctx, c.client, c.metaPath, func(task *models.PreImportTask) bool {
		return task.GetProto().GetJobID() == p.JobID
	})
	if err != nil {
		fmt.Println("failed to list preimport tasks, err=", err.Error())
		return err
	}

	// Query associated ImportTaskV2 tasks
	importTasks, err := common.ListImportTasks(ctx, c.client, c.metaPath, func(task *models.ImportTaskV2) bool {
		return task.GetProto().GetJobID() == p.JobID
	})
	if err != nil {
		fmt.Println("failed to list import tasks, err=", err.Error())
		return err
	}

	// Display summary of what will be removed
	fmt.Printf("\nWill remove the following items:\n")
	fmt.Printf("- Import Job: %d\n", p.JobID)
	if len(preimportTasks) > 0 {
		fmt.Printf("- PreImportTasks: %d\n", len(preimportTasks))
		for _, task := range preimportTasks {
			fmt.Printf("  - TaskID: %d, key=%s\n", task.GetProto().GetTaskID(), task.Key())
		}
	}
	if len(importTasks) > 0 {
		fmt.Printf("- ImportTaskV2: %d\n", len(importTasks))
		for _, task := range importTasks {
			fmt.Printf("  - TaskID: %d, key=%s\n", task.GetProto().GetTaskID(), task.Key())
		}
	}

	if !p.Run {
		return nil
	}

	// Start deletion process
	fmt.Printf("\nStart to delete import job and associated tasks...\n")

	preimportFailures := []int64{}
	importFailures := []int64{}

	// Delete PreImportTasks first
	for _, task := range preimportTasks {
		delCtx, cancel := context.WithTimeout(ctx, time.Second*3)
		err = c.client.Remove(delCtx, task.Key())
		cancel()
		if err != nil {
			fmt.Printf("failed to delete PreImportTask %d, error: %s\n", task.GetProto().GetTaskID(), err.Error())
			preimportFailures = append(preimportFailures, task.GetProto().GetTaskID())
		} else {
			fmt.Printf("removed PreImportTask %d done\n", task.GetProto().GetTaskID())
		}
	}

	// Delete ImportTaskV2 tasks
	for _, task := range importTasks {
		delCtx, cancel := context.WithTimeout(ctx, time.Second*3)
		err = c.client.Remove(delCtx, task.Key())
		cancel()
		if err != nil {
			fmt.Printf("failed to delete ImportTaskV2 %d, error: %s\n", task.GetProto().GetTaskID(), err.Error())
			importFailures = append(importFailures, task.GetProto().GetTaskID())
		} else {
			fmt.Printf("removed ImportTaskV2 %d done\n", task.GetProto().GetTaskID())
		}
	}

	// Delete the job itself (capture error, don't return immediately)
	fmt.Printf("Start to delete import job...\n")
	delCtx, cancel := context.WithTimeout(ctx, time.Second*3)
	jobDeletionError := c.client.Remove(delCtx, targetPath)
	cancel()

	// Always print deletion summary first (so users know what happened to tasks)
	if len(preimportFailures) > 0 || len(importFailures) > 0 {
		fmt.Printf("\nDeletion Summary:\n")
		if len(preimportFailures) > 0 {
			fmt.Printf("  Failed PreImportTasks: %v\n", preimportFailures)
		}
		if len(importFailures) > 0 {
			fmt.Printf("  Failed ImportTaskV2s: %v\n", importFailures)
		}
		fmt.Printf("  Successfully deleted: %d PreImportTasks, %d ImportTaskV2s\n",
			len(preimportTasks)-len(preimportFailures), len(importTasks)-len(importFailures))
	}

	// Now handle job deletion outcome
	if jobDeletionError != nil {
		fmt.Printf("failed to delete import job %d, error: %s\n", targetJob.GetJobID(), jobDeletionError.Error())
		if len(preimportFailures) > 0 || len(importFailures) > 0 {
			return fmt.Errorf("failed to delete import job and some tasks (see summary above)")
		}
		return fmt.Errorf("failed to delete import job (tasks were deleted, see summary above)")
	}

	fmt.Printf("Successfully deleted import job %d\n", targetJob.GetJobID())

	// Return error if any tasks failed, even though job succeeded
	if len(preimportFailures) > 0 || len(importFailures) > 0 {
		return fmt.Errorf("import job deleted but some tasks failed to delete (see summary above)")
	}

	return nil
}

func (c *ComponentRemove) RemoveImportTaskCommand(ctx context.Context, p *RemoveImportTaskParam) error {
	if p.TaskID == 0 {
		fmt.Println("Error: task id is required (use --task <taskID>)")
		return nil
	}

	// Search in PreImportTasks
	preimportTasks, err := common.ListPreImportTasks(ctx, c.client, c.metaPath, func(task *models.PreImportTask) bool {
		return task.GetProto().GetTaskID() == p.TaskID
	})
	if err != nil {
		fmt.Println("failed to list preimport tasks, err=", err.Error())
		return err
	}

	// Search in ImportTaskV2
	importTasks, err := common.ListImportTasks(ctx, c.client, c.metaPath, func(task *models.ImportTaskV2) bool {
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
