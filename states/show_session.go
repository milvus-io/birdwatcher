package states

import (
	"fmt"
	"path"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// getEtcdShowSession returns show session command.
// usage: show session
func getEtcdShowSession(cli *clientv3.Client, basePath string) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "session",
		Short:   "list online milvus components",
		Aliases: []string{"sessions"},
		RunE: func(cmd *cobra.Command, args []string) error {
			sessions, err := listSessions(cli, basePath)
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

// listSessions returns all session
func listSessions(cli *clientv3.Client, basePath string) ([]*models.Session, error) {
	prefix := path.Join(basePath, "session")
	return listSessionsByPrefix(cli, prefix)
}
