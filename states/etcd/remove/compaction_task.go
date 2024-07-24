package remove

import (
	"context"
	"fmt"
	"path"

	"github.com/spf13/cobra"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/birdwatcher/states/kv"
)

// CompactionTaskCleanCommand returns command to remove
func CompactionTaskCleanCommand(cli kv.MetaKV, basePath string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "compaction",
		Short: "Remove compaction task",
		Run: func(cmd *cobra.Command, args []string) {
			compactionType, err := cmd.Flags().GetString("type")
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			triggerID, err := cmd.Flags().GetString("jobID")
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			planID, err := cmd.Flags().GetString("taskID")
			if err != nil {
				fmt.Println(err.Error())
				return
			}

			compactionTasks, err := common.ListCompactionTask(context.TODO(), cli, basePath, func(task *models.CompactionTask) bool {
				if compactionType != task.GetType().String() {
					return false
				}
				if triggerID != "" && fmt.Sprint(task.GetTriggerID()) != triggerID {
					return false
				}
				if planID != "" && fmt.Sprint(task.GetPlanID()) != planID {
					return false
				}
				return true
			})
			if err != nil {
				fmt.Println(err.Error())
				return
			}

			for _, task := range compactionTasks {
				key := path.Join(basePath, common.CompactionTaskPrefix, task.GetType().String(), fmt.Sprint(task.GetTriggerID()), fmt.Sprint(task.GetPlanID()))
				err = cli.RemoveWithPrefix(context.TODO(), key)
				if err != nil {
					fmt.Println(err.Error())
					return
				}
				fmt.Printf("clean compaction task done, prefix: %s\n", key)
			}
		},
	}

	cmd.Flags().String("type", "ClusteringCompaction", "compaction type")
	cmd.Flags().String("jobID", "", "jobID also known as triggerID")
	cmd.Flags().String("taskID", "", "taskID also known as planID")
	return cmd
}
