package show

import (
	"context"
	"fmt"
	"path"

	"github.com/spf13/cobra"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/birdwatcher/states/kv"
)

const (
	queryNodeInfoPrefix = "queryCoord-queryNodeInfo"
)

func listQueryCoordClusterNodeInfo(ctx context.Context, cli kv.MetaKV, basePath string) ([]*models.Session, error) {
	prefix := path.Join(basePath, queryNodeInfoPrefix)
	return common.ListSessionsByPrefix(ctx, cli, prefix)
}

// QueryCoordClusterCommand returns show querycoord-cluster command.
func QueryCoordClusterCommand(cli kv.MetaKV, basePath string) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "querycoord-cluster",
		Short:   "display querynode information from querycoord cluster",
		Aliases: []string{"querycoord-clusters"},
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			sessions, err := listQueryCoordClusterNodeInfo(ctx, cli, basePath)
			if err != nil {
				fmt.Println("failed to list tasks in querycoord", err.Error())
				return nil
			}

			onlineSessons, _ := common.ListSessions(ctx, cli, basePath)
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
