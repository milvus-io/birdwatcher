package show

import (
	"fmt"

	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// SessionCommand returns show session command.
// usage: show session
func SessionCommand(cli clientv3.KV, basePath string) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "session",
		Short:   "list online milvus components",
		Aliases: []string{"sessions"},
		RunE: func(cmd *cobra.Command, args []string) error {
			sessions, err := common.ListSessions(cli, basePath)
			if err != nil {
				return err
			}
			for _, session := range sessions {
				fmt.Println(session.String())
			}
			return nil
		},
	}
	return cmd
}
