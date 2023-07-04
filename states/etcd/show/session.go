package show

import (
	"context"
	"fmt"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
)

type SessionParam struct {
	framework.ParamBase `use:"show session" desc:"list online milvus components" alias:"sessions"`
}

// SessionCommand returns show session command.
// usage: show session
func (c *ComponentShow) SessionCommand(ctx context.Context, p *SessionParam) error {
	sessions, err := common.ListSessions(c.client, c.basePath)
	if err != nil {
		return err
	}
	for _, session := range sessions {
		fmt.Println(session.String())
	}
	return nil
}

/*
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
}*/
