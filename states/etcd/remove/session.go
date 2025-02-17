package remove

import (
	"context"
	"fmt"
	"strings"

	"github.com/samber/lo"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
)

type RemoveSessionParam struct {
	framework.ParamBase `use:"remove session" desc:"remove session with specified component type & node id"`
	Component           string `name:"component" default:"" desc:"component type to remove"`
	ID                  int64  `name:"sessionID" default:"0" desc:"session id to remove"`
	Run                 bool   `name:"run" default:"false" desc:"actual remove session, default in dry-run mode"`
}

func (c *ComponentRemove) RemoveSessionCommand(ctx context.Context, p *RemoveSessionParam) error {
	sessions, err := common.ListSessions(c.client, c.basePath)
	if err != nil {
		return err
	}

	sessions = lo.Filter(sessions, func(s *models.Session, _ int) bool {
		return strings.EqualFold(s.ServerName, p.Component) && s.ServerID == p.ID
	})

	if len(sessions) == 0 {
		fmt.Printf("Session component type=%s session id=%d not found", p.Component, p.ID)
	}

	fmt.Printf("%d session item found:\n", len(sessions))
	for _, session := range sessions {
		fmt.Println(session.String())
		fmt.Println(session.GetKey())
	}

	if p.Run {
		fmt.Println("Start to remove session")
		for _, session := range sessions {
			err := c.client.Remove(ctx, session.GetKey())
			if err != nil {
				return err
			}
		}
	} else {
		fmt.Println("[Dry-mode] skip removing")
	}

	return nil
}
