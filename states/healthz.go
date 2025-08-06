package states

import (
	"context"
	"fmt"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/states/healthz"
)

type HealthzCheckParam struct {
	framework.ParamBase `use:"healthz-check" desc:"perform healthz check for connect instance"`
	Items               []string `name:"items" default:"" desc:"healthz check items"`
}

func (c *InstanceState) HealthzCheckCommand(ctx context.Context, p *HealthzCheckParam) (*framework.PresetResultSet, error) {
	var items []healthz.HealthzCheckItem
	if len(p.Items) == 0 {
		items = healthz.DefaultCheckItems()
	} else {
		items = make([]healthz.HealthzCheckItem, 0, len(p.Items))
		for _, name := range p.Items {
			item, ok := healthz.GetHealthzCheckItem(name)
			if !ok {
				return nil, errors.Newf("unknown healthz check item: %s", name)
			}
			items = append(items, item)
		}
	}

	var results []*healthz.HealthzCheckReport
	for _, item := range items {
		itemResults, err := item.Check(ctx, c.client, c.basePath)
		if err != nil {
			return nil, err
		}
		results = append(results, itemResults...)
	}

	return framework.NewPresetResultSet(framework.NewListResult[healthz.HealthzCheckReports](results), framework.FormatJSON), nil
}

type ListHealthzCheckParam struct {
	framework.ParamBase `use:"show healthz-checks" desc:"list available healthz check items"`
}

func (c *InstanceState) ListHealthzCheckCommand(ctx context.Context, p *ListHealthzCheckParam) error {
	items := healthz.AllCheckItems()
	for _, item := range items {
		fmt.Println("CheckItem:", item.Name())
		fmt.Println(item.Description())
		fmt.Println()
	}
	return nil
}
