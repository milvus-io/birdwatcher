package show

import (
	"context"
	"errors"
	"fmt"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"time"

	"github.com/spf13/cobra"

	"github.com/milvus-io/birdwatcher/states/kv"
)

func listQueryCoordTriggerTasks(cli kv.MetaKV, basePath string) (map[UniqueID]queryCoordTask, error) {
	prefix := path.Join(basePath, triggerTaskPrefix)
	triggerTasks, err := listQueryCoordTasksByPrefix(cli, prefix)
	if err != nil {
		return nil, err
	}
	for _, task := range triggerTasks {
		task.setType("Trigger")
	}
	return triggerTasks, nil
}

func listQueryCoordActivateTasks(cli kv.MetaKV, basePath string) (map[UniqueID]queryCoordTask, error) {
	prefix := path.Join(basePath, activeTaskPrefix)
	activateTasks, err := listQueryCoordTasksByPrefix(cli, prefix)
	if err != nil {
		return nil, err
	}
	for _, task := range activateTasks {
		task.setType("Activate")
	}
	return activateTasks, nil
}

func listQueryCoordTasksByPrefix(cli kv.MetaKV, prefix string) (map[UniqueID]queryCoordTask, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	tasks := make(map[int64]queryCoordTask)
	keys, vals, err := cli.LoadWithPrefix(ctx, prefix)
	if err != nil {
		return nil, err
	}
	if len(keys) != len(vals) {
		return nil, fmt.Errorf("unmatched kv sizes for %s: len(keys): %d, len(vals): %d", prefix, len(keys), len(vals))
	}
	for i, key := range keys {
		taskID, err := strconv.ParseInt(filepath.Base(key), 10, 64)
		if err != nil {
			return nil, err
		}
		t, err := unmarshalQueryTask(taskID, vals[i])
		if err != nil {
			return nil, err
		}
		tasks[taskID] = t
	}
	return tasks, nil
}

func listQueryCoordTaskStates(cli kv.MetaKV, basePath string) (map[UniqueID]taskState, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	prefix := path.Join(basePath, taskInfoPrefix)
	taskInfos := make(map[int64]taskState)
	keys, vals, err := cli.LoadWithPrefix(ctx, prefix)
	if err != nil {
		return nil, err
	}
	if len(keys) != len(vals) {
		return nil, fmt.Errorf("unmatched kv sizes for %s: len(keys): %d, len(vals): %d", prefix, len(keys), len(vals))
	}
	for i, key := range keys {
		taskID, err := strconv.ParseInt(filepath.Base(key), 10, 64)
		if err != nil {
			return nil, err
		}
		value, err := strconv.ParseInt(vals[i], 10, 64)
		if err != nil {
			return nil, err
		}
		state := taskState(value)
		taskInfos[taskID] = state
	}
	return taskInfos, nil
}

func checkAndSetTaskState(tasks map[UniqueID]queryCoordTask, states map[UniqueID]taskState) error {
	for tID := range tasks {
		state, ok := states[tID]
		if !ok {
			return errors.New("taskStateInfo and taskInfo are inconsistent")
		}
		tasks[tID].setState(state)
	}
	return nil
}

func listQueryCoordTasks(cli kv.MetaKV, basePath string, filter func(task queryCoordTask) bool) (map[UniqueID]queryCoordTask, map[UniqueID]queryCoordTask, error) {
	triggerTasks, err := listQueryCoordTriggerTasks(cli, basePath)
	if err != nil {
		return nil, nil, err
	}
	for tID, task := range triggerTasks {
		if !filter(task) {
			delete(triggerTasks, tID)
		}
	}

	activateTasks, err := listQueryCoordActivateTasks(cli, basePath)
	if err != nil {
		return triggerTasks, nil, err
	}
	for tID, task := range activateTasks {
		if !filter(task) {
			delete(activateTasks, tID)
		}
	}
	taskStates, err := listQueryCoordTaskStates(cli, basePath)
	if err != nil {
		return nil, nil, err
	}
	err = checkAndSetTaskState(triggerTasks, taskStates)
	if err != nil {
		errStr := fmt.Sprintf("%s%s", "triggerTask", err.Error())
		return nil, nil, errors.New(errStr)
	}
	err = checkAndSetTaskState(activateTasks, taskStates)
	if err != nil {
		errStr := fmt.Sprintf("%s%s", "activateTask", err.Error())
		return nil, nil, errors.New(errStr)
	}
	return triggerTasks, activateTasks, nil
}

// QueryCoordTasks returns show querycoord-tasks commands.
// DEPRECATED from milvus 2.2.0.
func QueryCoordTasks(cli kv.MetaKV, basePath string) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "querycoord-task",
		Short:   "display task information from querycoord",
		Aliases: []string{"querycoord-tasks"},
		RunE: func(cmd *cobra.Command, args []string) error {
			collID, err := cmd.Flags().GetInt64("collection")
			if err != nil {
				return err
			}

			taskType, err := cmd.Flags().GetString("type")
			if err != nil {
				return err
			}

			if taskType != "" && taskType != "activate" && taskType != "trigger" && taskType != "all" {
				fmt.Println("wrong taskType")
				return nil
			}
			triggerTasks, activateTasks, err := listQueryCoordTasks(cli, basePath, func(task queryCoordTask) bool {
				if task == nil {
					return false
				}
				collectionID := task.getCollectionID()
				return collID == 0 || collectionID == collID
			})
			if err != nil {
				fmt.Println("failed to list tasks in querycoord", err.Error())
				return nil
			}

			printFunc := func(tasks map[UniqueID]queryCoordTask) {
				var tIDs []UniqueID
				for tID := range tasks {
					tIDs = append(tIDs, tID)
				}
				sort.Slice(tIDs, func(i, j int) bool {
					return tIDs[i] < tIDs[j]
				})
				for _, tID := range tIDs {
					task := tasks[tID]
					fmt.Printf("%d, %s\n", task.getTaskID(), task.getType())

					taskStr := task.String()
					if len(taskStr) > 200 {
						taskStr = taskStr[:200]
					}
					fmt.Println(taskStr)
				}
			}

			if taskType == "" || taskType == "all" || taskType == "trigger" {
				printFunc(triggerTasks)
			}
			if taskType == "" || taskType == "all" || taskType == "activate" {
				printFunc(activateTasks)
			}
			return nil
		},
	}
	cmd.Flags().Int64("collection", 0, "collection id to filter with")
	cmd.Flags().String("type", "all", "filter task types [activate, trigger, all]")
	return cmd
}
