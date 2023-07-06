package show

import (
	"context"
	"fmt"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
)

type DatabaseParam struct {
	framework.ParamBase `use:"show database" desc:"display Database info from rootcoord meta"`
	DatabaseName        string `name:"name" default:"" desc:"database name to filter with"`
}

// DatabaseCommand returns show database comand.
func (c *ComponentShow) DatabaseCommand(ctx context.Context, p *DatabaseParam) {
	dbs, err := common.ListDatabase(ctx, c.client, c.basePath)
	if err != nil {
		fmt.Println("failed to list database info", err.Error())
		return
	}

	for _, db := range dbs {
		printDatabaseInfo(db)
	}

	fmt.Printf("--- Total Database(s): %d\n", len(dbs))
}

func printDatabaseInfo(db *models.Database) {
	fmt.Println("=============================")
	fmt.Printf("ID: %d\tName: %s\n", db.ID, db.Name)
	fmt.Printf("TenantID: %s\t State: %s\n", db.TenantID, db.State.String())
}
