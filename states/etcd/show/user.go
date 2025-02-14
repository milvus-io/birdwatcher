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

type UserParam struct {
	framework.ParamBase `use:"show user" desc:"display user info from rootcoord meta"`
	// DatabaseName        string `name:"name" default:"" desc:"database name to filter with"`
}

// DatabaseCommand returns show database comand.
func (c *ComponentShow) UserCommand(ctx context.Context, p *UserParam) (*Users, error) {
	users, err := common.ListUsers(ctx, c.client, c.basePath)
	if err != nil {
		fmt.Println("failed to list database info", err.Error())
		return nil, errors.Wrap(err, "failed to list database info")
	}

	return framework.NewListResult[Users](users), nil
}

type Users struct {
	framework.ListResultSet[*models.UserInfo]
}

func (rs *Users) PrintAs(format framework.Format) string {
	switch format {
	case framework.FormatDefault, framework.FormatPlain:
		sb := &strings.Builder{}
		for _, user := range rs.Data {
			// rs.printDatabaseInfo(sb, database)
			sb.WriteString(fmt.Sprintf("Username: %s Tenant:%s\n", user.Username, user.Tenant))
		}
		fmt.Fprintf(sb, "--- Total Users(s): %d\n", len(rs.Data))
		return sb.String()
	default:
	}
	return ""
}
