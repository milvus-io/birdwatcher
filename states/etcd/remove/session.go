package remove

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/birdwatcher/states/kv"
)

type RemoveSessionParam struct {
	framework.ExecutionParam `use:"remove session" desc:"remove session with specified component type & node id"`
	Component                string `name:"component" default:"" desc:"component type to remove"`
	ID                       int64  `name:"sessionID" default:"0" desc:"session id to remove"`
}

func (c *ComponentRemove) RemoveSessionCommand(ctx context.Context, p *RemoveSessionParam) error {
	sessions, err := common.ListSessions(ctx, c.client, c.basePath)
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
		fmt.Println("Start to revoke session lease")
		for _, session := range sessions {
			etcdCli := kv.MustGetETCDClient(c.client)
			if _, err := etcdCli.Lease.Revoke(ctx, clientv3.LeaseID(session.LeaseID)); err != nil && !errors.Is(err, rpctypes.ErrLeaseNotFound) {
				return err
			}
		}
		fmt.Println("Session lease revoked, validating session key to be removed...")
		var validationErrors []string

		for _, session := range sessions {
			_, err := c.client.Load(ctx, session.GetKey())
			if !errors.Is(err, kv.ErrKeyNotFound) {
				validationErrors = append(validationErrors, fmt.Sprintf("session key %s still exists", session.GetKey()))
			}
		}
		if len(validationErrors) > 0 {
			return fmt.Errorf("validation failed: %s", strings.Join(validationErrors, "; "))
		}
		fmt.Println("Session key removed successfully, done")
	} else {
		fmt.Println("[Dry-mode] skip removing")
	}
	return nil
}
