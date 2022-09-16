// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package states

import (
	"context"
	"errors"
	"fmt"
	"path"
	"path/filepath"
	"sort"
	"strconv"

	//"sort"
	"time"

	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func listQueryCoordTriggerTasks(cli *clientv3.Client, basePath string) (map[UniqueID]queryCoordTask, error) {
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

func listQueryCoordActivateTasks(cli *clientv3.Client, basePath string) (map[UniqueID]queryCoordTask, error) {
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

func listQueryCoordTasksByPrefix(cli *clientv3.Client, prefix string) (map[UniqueID]queryCoordTask, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	tasks := make(map[int64]queryCoordTask)
	resp, err := cli.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	for _, kv := range resp.Kvs {
		key := string(kv.Key)
		taskID, err := strconv.ParseInt(filepath.Base(key), 10, 64)
		if err != nil {
			return nil, err
		}
		valueStr := string(kv.Value)
		t, err := unmarshalQueryTask(taskID, valueStr)
		if err != nil {
			return nil, err
		}
		tasks[taskID] = t
	}
	return tasks, nil
}

func listQueryCoordTaskStates(cli *clientv3.Client, basePath string) (map[UniqueID]taskState, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	prefix := path.Join(basePath, taskInfoPrefix)
	taskInfos := make(map[int64]taskState)
	resp, err := cli.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	for _, kv := range resp.Kvs {
		key := string(kv.Key)
		taskID, err := strconv.ParseInt(filepath.Base(key), 10, 64)
		if err != nil {
			return nil, err
		}
		valueStr := string(kv.Value)
		value, err := strconv.ParseInt(valueStr, 10, 64)
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

func listQueryCoordTasks(cli *clientv3.Client, basePath string, filter func(task queryCoordTask) bool) (map[UniqueID]queryCoordTask, map[UniqueID]queryCoordTask, error) {
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

func getQueryCoordTaskCmd(cli *clientv3.Client, basePath string) *cobra.Command {
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
				fmt.Println("worng taskType")
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
					fmt.Printf("%s\n\n", task.String())
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
