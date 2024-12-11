package remove

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/spf13/cobra"

	"github.com/milvus-io/birdwatcher/proto/v2.2/datapb"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/birdwatcher/states/etcd/show"
	"github.com/milvus-io/birdwatcher/states/kv"
)

// ImportJob returns remove import job command.
func ImportJob(cli kv.MetaKV, basePath string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "import-job",
		Short: "Remove import job from datacoord meta with specified job id",
		Run: func(cmd *cobra.Command, args []string) {
			jobIDStr, err := cmd.Flags().GetString("job")
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			jobID, err := strconv.ParseInt(jobIDStr, 10, 64)
			if err != nil {
				fmt.Printf("invalid jobID %s, err=%v\n", jobIDStr, err)
				return
			}

			run, err := cmd.Flags().GetBool("run")
			if err != nil {
				fmt.Println(err.Error())
				return
			}

			jobs, paths, err := common.ListImportJobs(context.Background(), cli, basePath, func(job *datapb.ImportJob) bool {
				return job.GetJobID() == jobID
			})
			if err != nil {
				fmt.Println("failed to list bulkinsert jobs, err=", err.Error())
				return
			}
			if len(jobs) != len(paths) {
				fmt.Printf("unaligned jobs and paths, len(jobs)=%d, len(paths)=%d", len(jobs), len(paths))
				return
			}
			if len(jobs) == 0 {
				fmt.Printf("cannot find target import job %s\n", jobIDStr)
				return
			}
			if len(jobs) > 1 {
				fmt.Printf("unexpected import job, expect 1, but got %d\n", len(jobs))
				return
			}

			targetJob := jobs[0]
			targetPath := paths[0]
			fmt.Printf("selected target import job, jobID: %d, key=%s\n", targetJob.GetJobID(), targetPath)
			show.PrintDetailedImportJob(context.Background(), cli, basePath, targetJob, false)

			if !run {
				return
			}

			fmt.Printf("Start to delete import job...\n")
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
			err = cli.Remove(ctx, targetPath)
			cancel()
			if err != nil {
				fmt.Printf("failed to delete import job %d, error: %s\n", targetJob.GetJobID(), err.Error())
				return
			}
			fmt.Printf("remove import job %d done\n", targetJob.GetJobID())
		},
	}

	cmd.Flags().Bool("run", false, "flags indicating whether to remove import job from meta")
	cmd.Flags().String("job", "", "import job id to remove")
	return cmd
}
