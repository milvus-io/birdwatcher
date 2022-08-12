package states

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"time"

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
				printSession(session)
			}
			return nil
		},
	}
	return cmd
}

// listSessions returns all session
func listSessions(cli *clientv3.Client, basePath string) ([]*models.Session, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	resp, err := cli.Get(ctx, path.Join(basePath, "session"), clientv3.WithPrefix())
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

func printSession(session *models.Session) {
	fmt.Printf("Session:%s, ServerID: %d\n", session.ServerName, session.ServerID)
}
