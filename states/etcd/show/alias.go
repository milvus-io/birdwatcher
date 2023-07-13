package show

import (
	"context"
	"fmt"

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
func (c *ComponentShow) AliasCommand(ctx context.Context, p *AliasParam) error {
	aliases, err := common.ListAliasVersion(ctx, c.client, c.basePath, etcdversion.GetVersion(), func(a *models.Alias) bool {
		return p.DBID == -1 || p.DBID == a.DBID
	})

	if err != nil {
		return err
	}

	for dbid, aliases := range lo.GroupBy(aliases, func(a *models.Alias) int64 { return a.DBID }) {
		fmt.Println("==========================")
		fmt.Println("Database ID: ", dbid)
		for _, alias := range aliases {
			c.PrintAlias(alias)
		}
	}

	return nil
}

func (c *ComponentShow) PrintAlias(a *models.Alias) {
	fmt.Printf("Collection ID: %d\tAlias Name: %s\tState: %s\n", a.CollectionID, a.Name, a.State.String())
}
