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
	framework.DataSetParam `use:"show resource-group" desc:"list resource groups in current instance"`
	Name                   string `name:"name" default:"" desc:"resource group name to list"`
}

func (c *ComponentShow) ResourceGroupCommand(ctx context.Context, p *ResourceGroupParam) (*framework.PresetResultSet, error) {
	rgs, err := common.ListResourceGroups(ctx, c.client, c.metaPath, func(rg *models.ResourceGroup) bool {
		return p.Name == "" || p.Name == rg.GetProto().GetName()
	})
	if err != nil {
		return nil, err
	}

	return framework.NewPresetResultSet(framework.NewListResult[ResourceGroups](rgs), framework.NameFormat(p.Format)), nil
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
	case framework.FormatJSON:
		return rs.printAsJSON()
	}
	return ""
}

func (rs *ResourceGroups) printAsJSON() string {
	type ResourceGroupJSON struct {
		Name           string  `json:"name"`
		CapacityLegacy int32   `json:"capacity_legacy"`
		Nodes          []int64 `json:"nodes"`
		LimitNodeNum   int32   `json:"limit_node_num"`
		RequestNodeNum int32   `json:"request_node_num"`
	}

	type OutputJSON struct {
		ResourceGroups []ResourceGroupJSON `json:"resource_groups"`
		Total          int                 `json:"total"`
	}

	output := OutputJSON{
		ResourceGroups: make([]ResourceGroupJSON, 0, len(rs.Data)),
		Total:          len(rs.Data),
	}

	for _, info := range rs.Data {
		rg := info.GetProto()
		output.ResourceGroups = append(output.ResourceGroups, ResourceGroupJSON{
			Name:           rg.GetName(),
			CapacityLegacy: rg.GetCapacity(),
			Nodes:          rg.GetNodes(),
			LimitNodeNum:   rg.GetConfig().GetLimits().GetNodeNum(),
			RequestNodeNum: rg.GetConfig().GetRequests().GetNodeNum(),
		})
	}

	return framework.MarshalJSON(output)
}
