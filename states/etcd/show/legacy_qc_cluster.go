package show

import (
	"fmt"
	"path"

	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
)

const (
	queryNodeInfoPrefix = "queryCoord-queryNodeInfo"
)

func listQueryCoordClusterNodeInfo(cli clientv3.KV, basePath string) ([]*models.Session, error) {
	prefix := path.Join(basePath, queryNodeInfoPrefix)
	return common.ListSessionsByPrefix(cli, prefix)
}

// QueryCoordClusterCommand returns show querycoord-cluster command.
func QueryCoordClusterCommand(cli clientv3.KV, basePath string) *cobra.Command {
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

			onlineSessons, _ := common.ListSessions(cli, basePath)
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
