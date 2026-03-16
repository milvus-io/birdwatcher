package show

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"text/tabwriter"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"

	"github.com/jedib0t/go-pretty/v6/table"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/rgpb"
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

	sessions, err := common.ListSessions(ctx, c.client, c.metaPath)
	if err != nil {
		return nil, errors.Wrap(err, "failed to list sessions")
	}

	rs := framework.NewListResult[ResourceGroups](rgs)
	rs.sessionMap = lo.SliceToMap(sessions, func(s *models.Session) (int64, *models.Session) { return s.ServerID, s })
	return framework.NewPresetResultSet(rs, framework.NameFormat(p.Format)), nil
}

type ResourceGroups struct {
	framework.ListResultSet[*models.ResourceGroup]
	sessionMap map[int64]*models.Session
}

func (rs *ResourceGroups) getHostName(nodeID int64) string {
	if sess, ok := rs.sessionMap[nodeID]; ok {
		return sess.HostName
	}
	return "NotFound"
}

func (rs *ResourceGroups) TableHeaders() table.Row {
	return table.Row{"Name", "Capacity", "Nodes"}
}

func (rs *ResourceGroups) TableRows() []table.Row {
	rows := make([]table.Row, 0, len(rs.Data))
	for _, info := range rs.Data {
		rg := info.GetProto()
		rows = append(rows, table.Row{rg.GetName(), rg.GetCapacity(), fmt.Sprintf("%v", rg.GetNodes())})
	}
	return rows
}

func (rs *ResourceGroups) PrintAs(format framework.Format) string {
	switch format {
	case framework.FormatDefault, framework.FormatPlain:
		sb := &strings.Builder{}
		tw := tabwriter.NewWriter(sb, 0, 0, 2, ' ', 0)
		for _, info := range rs.Data {
			rg := info.GetProto()
			fmt.Fprintf(tw, "Resource Group: %s\n", rg.GetName())
			// Config
			cfg := rg.GetConfig()
			fmt.Fprintf(tw, "  Config:\n")
			fmt.Fprintf(tw, "    Request: %d\tLimit: %d\n", cfg.GetRequests().GetNodeNum(), cfg.GetLimits().GetNodeNum())
			if transferFrom := cfg.GetTransferFrom(); len(transferFrom) > 0 {
				fmt.Fprintf(tw, "    TransferFrom: %v\n", lo.Map(transferFrom, func(t *rgpb.ResourceGroupTransfer, _ int) string { return t.GetResourceGroup() }))
			}
			if transferTo := cfg.GetTransferTo(); len(transferTo) > 0 {
				fmt.Fprintf(tw, "    TransferTo: %v\n", lo.Map(transferTo, func(t *rgpb.ResourceGroupTransfer, _ int) string { return t.GetResourceGroup() }))
			}
			if nodeFilter := cfg.GetNodeFilter(); nodeFilter != nil && len(nodeFilter.GetNodeLabels()) > 0 {
				labels := lo.Map(nodeFilter.GetNodeLabels(), func(kv *commonpb.KeyValuePair, _ int) string {
					return fmt.Sprintf("%s=%s", kv.GetKey(), kv.GetValue())
				})
				fmt.Fprintf(tw, "    NodeFilter: [%s]\n", strings.Join(labels, ", "))
			}
			// Nodes
			nodes := append([]int64{}, rg.GetNodes()...)
			slices.Sort(nodes)
			fmt.Fprintf(tw, "  Nodes (%d):\n", len(nodes))
			for _, nodeID := range nodes {
				fmt.Fprintf(tw, "    - %d\t%s\n", nodeID, rs.getHostName(nodeID))
			}
			fmt.Fprintln(tw)
		}
		tw.Flush()
		fmt.Fprintf(sb, "--- Total Resource Group(s): %d\n", len(rs.Data))
		return sb.String()
	case framework.FormatJSON:
		return rs.printAsJSON()
	}
	return ""
}

func (rs *ResourceGroups) printAsJSON() string {
	type NodeInfoJSON struct {
		NodeID   int64  `json:"node_id"`
		HostName string `json:"hostname"`
	}

	type ConfigJSON struct {
		RequestNodeNum int32    `json:"request_node_num"`
		LimitNodeNum   int32    `json:"limit_node_num"`
		TransferFrom   []string `json:"transfer_from,omitempty"`
		TransferTo     []string `json:"transfer_to,omitempty"`
		NodeLabels     []string `json:"node_labels,omitempty"`
	}

	type ResourceGroupJSON struct {
		Name   string       `json:"name"`
		Config ConfigJSON   `json:"config"`
		Nodes  []NodeInfoJSON `json:"nodes"`
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
		cfg := rg.GetConfig()

		cfgJSON := ConfigJSON{
			RequestNodeNum: cfg.GetRequests().GetNodeNum(),
			LimitNodeNum:   cfg.GetLimits().GetNodeNum(),
		}
		if transferFrom := cfg.GetTransferFrom(); len(transferFrom) > 0 {
			cfgJSON.TransferFrom = lo.Map(transferFrom, func(t *rgpb.ResourceGroupTransfer, _ int) string { return t.GetResourceGroup() })
		}
		if transferTo := cfg.GetTransferTo(); len(transferTo) > 0 {
			cfgJSON.TransferTo = lo.Map(transferTo, func(t *rgpb.ResourceGroupTransfer, _ int) string { return t.GetResourceGroup() })
		}
		if nodeFilter := cfg.GetNodeFilter(); nodeFilter != nil && len(nodeFilter.GetNodeLabels()) > 0 {
			cfgJSON.NodeLabels = lo.Map(nodeFilter.GetNodeLabels(), func(kv *commonpb.KeyValuePair, _ int) string {
				return fmt.Sprintf("%s=%s", kv.GetKey(), kv.GetValue())
			})
		}

		sortedNodes := append([]int64{}, rg.GetNodes()...)
		slices.Sort(sortedNodes)
		nodes := make([]NodeInfoJSON, 0, len(sortedNodes))
		for _, nodeID := range sortedNodes {
			nodes = append(nodes, NodeInfoJSON{NodeID: nodeID, HostName: rs.getHostName(nodeID)})
		}

		output.ResourceGroups = append(output.ResourceGroups, ResourceGroupJSON{
			Name:   rg.GetName(),
			Config: cfgJSON,
			Nodes:  nodes,
		})
	}

	return framework.MarshalJSON(output)
}
