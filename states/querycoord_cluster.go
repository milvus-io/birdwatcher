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
	"fmt"
	"path"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	queryNodeInfoPrefix = "queryCoord-queryNodeInfo"
)

func listQueryCoordClusterNodeInfo(cli *clientv3.Client, basePath string) ([]*models.Session, error) {
	prefix := path.Join(basePath, queryNodeInfoPrefix)
	return listSessionsByPrefix(cli, prefix)
}

func getQueryCoordClusterNodeInfo(cli *clientv3.Client, basePath string) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "querycoord-cluster",
		Short:   "display querynode information from querycoord cluster",
		Aliases: []string{"querycoord-clusters"},
		RunE: func(cmd *cobra.Command, args []string) error {
			sessions, err := listQueryCoordClusterNodeInfo(cli, basePath)
			if err != nil {
				fmt.Println("failed to list tasks in querycoord", err.Error())
				return nil
			}

			onlineSessons, _ := listSessions(cli, basePath)
			onlineSessionMap := make(map[UniqueID]struct{})
			for _, s := range onlineSessons {
				onlineSessionMap[s.ServerID] = struct{}{}
			}
			for _, s := range sessions {
				onlineStr := "Online"
				_, ok := onlineSessionMap[s.ServerID]
				if !ok {
					onlineStr = "Offline"
				}
				line := fmt.Sprintf("%s %s", s.String(), onlineStr)
				fmt.Println(line)
			}
			return nil
		},
	}
	return cmd
}
