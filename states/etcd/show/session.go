package show

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
)

type SessionParam struct {
	framework.ParamBase `use:"show session" desc:"list online milvus components" alias:"sessions"`
}

// SessionCommand returns show session command.
// usage: show session
func (c *ComponentShow) SessionCommand(ctx context.Context, p *SessionParam) (*Sessions, error) {
	sessions, err := common.ListSessions(c.client, c.basePath)
	if err != nil {
		return nil, errors.Wrap(err, "failed to list sessions")
	}

	return framework.NewListResult[Sessions](sessions), nil
}

type Sessions struct {
	framework.ListResultSet[*models.Session]
}

func (rs *Sessions) PrintAs(format framework.Format) string {
	switch format {
	case framework.FormatDefault, framework.FormatPlain:
		sb := &strings.Builder{}
		for _, session := range rs.Data {
			fmt.Fprintln(sb, session.String())
		}
		return sb.String()
	default:
	}
	return ""
}
