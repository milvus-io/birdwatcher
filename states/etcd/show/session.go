package show

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/fatih/color"
	"github.com/samber/lo"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
)

type SessionParam struct {
	framework.ParamBase `use:"show session" desc:"list online milvus components" alias:"sessions"`
	Format              string `name:"format" default:"line" desc:"output format"`
}

// SessionCommand returns show session command.
// usage: show session
func (c *ComponentShow) SessionCommand(ctx context.Context, p *SessionParam) (*framework.PresetResultSet, error) {
	sessions, err := common.ListSessions(ctx, c.client, c.metaPath)
	if err != nil {
		return nil, errors.Wrap(err, "failed to list sessions")
	}

	return framework.NewPresetResultSet(framework.NewListResult[Sessions](sessions), framework.NameFormat(p.Format)), nil
}

type Sessions struct {
	framework.ListResultSet[*models.Session]
}

type SessionGroup struct{}

func (rs *Sessions) PrintAs(format framework.Format) string {
	switch format {
	case framework.FormatDefault, framework.FormatPlain:
		return rs.printAsGroups()
	case framework.FormatJSON:
		return rs.PrintAsJSON()
	default:
	}
	return ""
}

func (rs *Sessions) PrintAsJSON() string {
	bs, err := json.Marshal(rs.Data)
	if err != nil {
		return err.Error()
	}
	return string(bs)
}

func (rs *Sessions) printAsGroups() string {
	sb := &strings.Builder{}

	sessionGroups := lo.GroupBy(rs.Data, func(session *models.Session) int64 {
		return session.ServerID
	})

	componentGroups := lo.GroupBy(rs.Data, func(session *models.Session) string {
		return session.ServerName
	})

	isMixture := func(session *models.Session) string {
		sessions := sessionGroups[session.ServerID]
		if len(sessions) > 1 {
			return color.BlueString("[Mixture]")
		}
		return ""
	}

	coords := []string{"rootcoord", "datacoord", "querycoord", "indexcoord"}
	if _, ok := componentGroups["mixcoord"]; ok {
		// after 2.6, all coordinators are merged into one mixcoord session
		coords = []string{"mixcoord"}
	}

	for _, coord := range coords {
		fmt.Fprintf(sb, "Cordinator %s\n", color.GreenString(coord))
		sessions := componentGroups[coord]
		main := lo.FindOrElse(sessions, nil, func(session *models.Session) bool {
			return session.IsMain(coord)
		})
		if main != nil {
			fmt.Fprintf(sb, "%s\tID: %d%s\tVersion: %s\tAddress: %s\n", color.GreenString("[Main]"), main.ServerID, isMixture(main), main.Version, main.Address)
		}
		standBys := lo.Filter(sessions, func(session *models.Session, _ int) bool {
			return main == nil || session.ServerID != main.ServerID
		})
		for _, standBy := range standBys {
			fmt.Fprintf(sb, "%s\tID: %d%s\tVersion: %s\tAddress: %s\n", color.YellowString("[Stand]"), standBy.ServerID, isMixture(standBy), standBy.Version, standBy.Address)
		}
		fmt.Fprintln(sb)
	}

	for _, node := range []string{"datanode", "querynode", "indexnode", "proxy", "streamingnode"} {
		fmt.Fprintf(sb, "Node(s) %s\n", color.GreenString(node))
		sessions := componentGroups[node]
		for _, session := range sessions {
			fmt.Fprintf(sb, "\tID: %d\tVersion: %s\tAddress: %s\n", session.ServerID, session.Version, session.Address)
		}
		fmt.Fprintln(sb)
	}

	return sb.String()
}
