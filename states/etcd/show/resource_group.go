package show

import (
	"context"
	"fmt"
	"strings"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
)

type ResourceGroupParam struct {
	framework.ParamBase `use:"show resource-group" desc:"list resource groups in current instance"`
	Name                string `name:"name" default:"" desc:"resource group name to list"`
}

func (c *ComponentShow) ResourceGroupCommand(ctx context.Context, p *ResourceGroupParam) (*ResourceGroups, error) {
	rgs, err := common.ListResourceGroups(ctx, c.client, c.metaPath, func(rg *models.ResourceGroup) bool {
		return p.Name == "" || p.Name == rg.GetProto().GetName()
	})
	if err != nil {
		return nil, err
	}

	return framework.NewListResult[ResourceGroups](rgs), nil
}

type ResourceGroups struct {
	framework.ListResultSet[*models.ResourceGroup]
}

func (rs *ResourceGroups) PrintAs(format framework.Format) string {
	switch format {
	case framework.FormatDefault, framework.FormatPlain:
		sb := &strings.Builder{}
		for _, info := range rs.Data {
			rg := info.GetProto()
			fmt.Fprintf(sb, "Resource Group Name: %s\tCapacity[Legacy]: %d\tNodes: %v\tLimit: %d\tRequest: %d\n", rg.GetName(), rg.GetCapacity(), rg.GetNodes(), rg.GetConfig().GetLimits().GetNodeNum(), rg.GetConfig().GetRequests().GetNodeNum())
		}
		fmt.Fprintf(sb, "--- Total Resource Group(s): %d\n", len(rs.Data))
		return sb.String()
	default:
	}
	return ""
}
