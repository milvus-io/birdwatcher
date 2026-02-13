package show

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	etcdversion "github.com/milvus-io/birdwatcher/states/etcd/version"
	"github.com/milvus-io/birdwatcher/utils"
)

type AliasParam struct {
	framework.DataSetParam `use:"show alias" desc:"list alias meta info" alias:"aliases"`
	DBID                   int64 `name:"dbid" default:"-1" desc:"database id to filter with"`
}

// AliasCommand implements `show alias` command.
func (c *ComponentShow) AliasCommand(ctx context.Context, p *AliasParam) (*framework.PresetResultSet, error) {
	aliases, err := common.ListAlias(ctx, c.client, c.metaPath, etcdversion.GetVersion(), func(a *models.Alias) bool {
		return p.DBID == -1 || p.DBID == a.DBID
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to list alias info")
	}

	return framework.NewPresetResultSet(framework.NewListResult[Aliases](aliases), framework.NameFormat(p.Format)), nil
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
	case framework.FormatJSON:
		return rs.printAsJSON()
	default:
	}
	return ""
}

func (rs *Aliases) printAsJSON() string {
	type AliasJSON struct {
		DBID            int64  `json:"db_id"`
		CollectionID    int64  `json:"collection_id"`
		Name            string `json:"name"`
		State           string `json:"state"`
		CreateTimestamp string `json:"create_timestamp"`
	}

	type OutputJSON struct {
		Aliases []AliasJSON `json:"aliases"`
		Total   int         `json:"total"`
	}

	output := OutputJSON{
		Aliases: make([]AliasJSON, 0, len(rs.Data)),
		Total:   len(rs.Data),
	}

	for _, a := range rs.Data {
		t, _ := utils.ParseTS(a.CreateTS)
		output.Aliases = append(output.Aliases, AliasJSON{
			DBID:            a.DBID,
			CollectionID:    a.CollectionID,
			Name:            a.Name,
			State:           a.State.String(),
			CreateTimestamp: t.Format("2006-01-02 15:04:05"),
		})
	}

	return framework.MarshalJSON(output)
}

func (rs *Aliases) PrintAlias(sb *strings.Builder, a *models.Alias) {
	t, _ := utils.ParseTS(a.CreateTS)
	fmt.Printf("Collection ID: %d\tAlias Name: %s\tState: %s\tCreateTimestamp: %v\n", a.CollectionID, a.Name, a.State.String(), t)
}
