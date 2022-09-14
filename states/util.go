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
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	logicalBits     = 18
	logicalBitsMask = (1 << logicalBits) - 1
)

func getGlobalUtilCommands() []*cobra.Command {
	return []*cobra.Command{
		getParseTSCmd(),
	}
}

func ParseTS(ts uint64) (time.Time, uint64) {
	logical := ts & logicalBitsMask
	physical := ts >> logicalBits
	physicalTime := time.Unix(int64(physical/1000), int64(physical)%1000*time.Millisecond.Nanoseconds())
	return physicalTime, logical
}

// listSessions returns all session
func listSessionsByPrefix(cli *clientv3.Client, prefix string) ([]*models.Session, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	resp, err := cli.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	sessions := make([]*models.Session, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		session := &models.Session{}
		err := json.Unmarshal(kv.Value, session)
		if err != nil {
			continue
		}

		sessions = append(sessions, session)
	}
	return sessions, nil
}

// getParseTSCmd returns command for parse timestamp
func getParseTSCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "parse-ts",
		Short: "parse hybrid timestamp",
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) == 0 {
				fmt.Println("no ts provided")
				return
			}

			for _, arg := range args {
				ts, err := strconv.ParseUint(arg, 10, 64)
				if err != nil {
					fmt.Printf("failed to parse ts from %s, err: %s\n", arg, err.Error())
					continue
				}

				t, _ := ParseTS(ts)
				fmt.Printf("Parse ts result, ts:%d, time: %v\n", ts, t)
			}
		},
	}
	return cmd
}
