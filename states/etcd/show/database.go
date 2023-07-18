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

type DatabaseParam struct {
	framework.ParamBase `use:"show database" desc:"display Database info from rootcoord meta"`
	DatabaseName        string `name:"name" default:"" desc:"database name to filter with"`
}

// DatabaseCommand returns show database comand.
func (c *ComponentShow) DatabaseCommand(ctx context.Context, p *DatabaseParam) (*Databases, error) {
	dbs, err := common.ListDatabase(ctx, c.client, c.basePath)
	if err != nil {
		fmt.Println("failed to list database info", err.Error())
		return nil, errors.Wrap(err, "failed to list database info")
	}

	return framework.NewListResult[Databases](dbs), nil
}

type Databases struct {
	framework.ListResultSet[*models.Database]
}

func (rs *Databases) PrintAs(format framework.Format) string {
	switch format {
	case framework.FormatDefault, framework.FormatPlain:
		sb := &strings.Builder{}
		for _, database := range rs.Data {
			rs.printDatabaseInfo(sb, database)
		}
		fmt.Fprintf(sb, "--- Total Database(s): %d\n", len(rs.Data))
		return sb.String()
	default:
	}
	return ""
}

func (rs *Databases) printDatabaseInfo(sb *strings.Builder, db *models.Database) {
	fmt.Fprintln(sb, "=============================")
	fmt.Fprintf(sb, "ID: %d\tName: %s\n", db.ID, db.Name)
	fmt.Fprintf(sb, "TenantID: %s\t State: %s\n", db.TenantID, db.State.String())
}
