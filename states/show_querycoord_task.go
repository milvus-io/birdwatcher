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
	"fmt"
	"path"
	"path/filepath"
	"strconv"

	//"sort"
	"time"

	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"
)

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

func listQueryCoordTaskStates(cli *clientv3.Client, prefix string) (map[UniqueID]taskState, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
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

func listQueryCoordTasks(cli *clientv3.Client, basePath string, filter func(task queryCoordTask) bool) (map[UniqueID]queryCoordTask, map[UniqueID]queryCoordTask, error) {
	prefix := path.Join(basePath, triggerTaskPrefix)
	triggerTasks, err := listQueryCoordTasksByPrefix(cli, prefix)
	if err != nil {
		return nil, nil, err
	}

	for tID, task := range triggerTasks {
		if !filter(task) {
			delete(triggerTasks, tID)
		}
	}

	prefix = path.Join(basePath, taskInfoPrefix)
	triggerTaskStates, err := listQueryCoordTaskStates(cli, prefix)
	if err != nil {
		return nil, nil, err
	}

	for tID, tState := range triggerTaskStates {
		if task, ok := triggerTasks[tID]; !ok {
			if filter(task) {
				fmt.Println("reloadFromKV: taskStateInfo and triggerTaskInfo are inconsistent")
				continue
			}
		} else {
			triggerTasks[tID].setState(tState)
		}
	}

	prefix = path.Join(basePath, activeTaskPrefix)
	activateTasks, err := listQueryCoordTasksByPrefix(cli, prefix)
	if err != nil {
		return triggerTasks, nil, err
	}
	for tID, task := range activateTasks {
		if !filter(task) {
			delete(activateTasks, tID)
		}
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
				return (collID == 0 || collectionID == collID)
			})
			if err != nil {
				fmt.Println("failed to list tasks in querycoord", err.Error())
				return nil
			}
			if taskType == "" || taskType == "all" || taskType == "trigger" {
				for tID, task := range triggerTasks {
					fmt.Printf("===trigger %d===\n", tID)
					fmt.Printf("%s\n", task.String())
					fmt.Printf("======\n")
				}
			}

			if taskType == "" || taskType == "all" || taskType == "activate" {
				for tID, task := range activateTasks {
					fmt.Printf("---activate %d---\n", tID)
					fmt.Printf("%s\n", task.String())
					fmt.Printf("------\n")
				}
			}
			return nil
		},
	}
	cmd.Flags().Int64("collection", 0, "collection id to filter with")
	cmd.Flags().String("type", "all", "filter task types [activate, trigger, all]")
	return cmd
}
