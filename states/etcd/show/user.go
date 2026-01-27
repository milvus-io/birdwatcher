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
	Format              string `name:"format" default:"" desc:"output format (default, json)"`
}

// UserCommand returns show user command.
func (c *ComponentShow) UserCommand(ctx context.Context, p *UserParam) (*framework.PresetResultSet, error) {
	users, err := common.ListUsers(ctx, c.client, c.basePath)
	if err != nil {
		return nil, errors.Wrap(err, "failed to list user info")
	}

	return framework.NewPresetResultSet(framework.NewListResult[Users](users), framework.NameFormat(p.Format)), nil
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
	case framework.FormatJSON:
		return rs.printAsJSON()
	default:
	}
	return ""
}

func (rs *Users) printAsJSON() string {
	type UserJSON struct {
		Username string `json:"username"`
		Tenant   string `json:"tenant"`
	}

	type OutputJSON struct {
		Users []UserJSON `json:"users"`
		Total int        `json:"total"`
	}

	output := OutputJSON{
		Users: make([]UserJSON, 0, len(rs.Data)),
		Total: len(rs.Data),
	}

	for _, user := range rs.Data {
		output.Users = append(output.Users, UserJSON{
			Username: user.Username,
			Tenant:   user.Tenant,
		})
	}

	return framework.MarshalJSON(output)
}
