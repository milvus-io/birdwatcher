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
