package show

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	etcdversion "github.com/milvus-io/birdwatcher/states/etcd/version"
	"github.com/samber/lo"
)

type AliasParam struct {
	framework.ParamBase `use:"show alias" desc:"list alias meta info" alias:"aliases"`
	DBID                int64 `name:"dbid" default:"-1" desc:"database id to filter with"`
}

// AliasCommand implements `show alias` command.
func (c *ComponentShow) AliasCommand(ctx context.Context, p *AliasParam) (*Aliases, error) {
	aliases, err := common.ListAliasVersion(ctx, c.client, c.basePath, etcdversion.GetVersion(), func(a *models.Alias) bool {
		return p.DBID == -1 || p.DBID == a.DBID
	})

	if err != nil {
		return nil, errors.Wrap(err, "failed to list alias info")
	}

	return framework.NewListResult[Aliases](aliases), nil
}

type Aliases struct {
	framework.ListResultSet[*models.Alias]
}

func (rs *Aliases) PrintAs(format framework.Format) string {
	switch format {
	case framework.FormatDefault, framework.FormatPlain:
		sb := &strings.Builder{}
		for dbid, aliases := range lo.GroupBy(rs.Data, func(a *models.Alias) int64 { return a.DBID }) {
			fmt.Fprintln(sb, "==========================")
			fmt.Println(sb, "Database ID: ", dbid)
			for _, alias := range aliases {
				rs.PrintAlias(sb, alias)
			}
		}
		return sb.String()
	default:
	}
	return ""
}

func (rs *Aliases) Entities() any {
	return rs.Data
}

func (rs *Aliases) PrintAlias(sb *strings.Builder, a *models.Alias) {
	fmt.Printf("Collection ID: %d\tAlias Name: %s\tState: %s\n", a.CollectionID, a.Name, a.State.String())
}
