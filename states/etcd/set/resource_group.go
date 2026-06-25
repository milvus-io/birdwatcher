package set

import (
	"context"
	"fmt"
	"math"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/birdwatcher/states/kv"
	"github.com/milvus-io/milvus-proto/go-api/v2/rgpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
)

const resourceGroupNodeNumUnset int64 = -1

type ResourceGroupParam struct {
	framework.ExecutionParam `use:"set resource-group" desc:"set resource group request and limit node num in etcd"`
	Name                     string `name:"name" default:"" desc:"resource group name to update"`
	Request                  int64  `name:"request" default:"-1" desc:"request node num, -1 means unchanged"`
	Limit                    int64  `name:"limit" default:"-1" desc:"limit node num, -1 means unchanged"`
}

func (c *ComponentSet) SetResourceGroupCommand(ctx context.Context, p *ResourceGroupParam) error {
	if p.Name == "" {
		return fmt.Errorf("resource group name cannot be empty")
	}
	if err := validateResourceGroupNodeNum("request", p.Request); err != nil {
		return err
	}
	if err := validateResourceGroupNodeNum("limit", p.Limit); err != nil {
		return err
	}
	if p.Request == resourceGroupNodeNumUnset && p.Limit == resourceGroupNodeNumUnset {
		return fmt.Errorf("at least one of --request or --limit must be set")
	}

	_, after, changed, err := updateResourceGroupNodeNums(ctx, c.client, c.basePath, p.Name, p.Request, p.Limit, p.IsDryRun())
	if err != nil {
		return err
	}
	if !changed {
		fmt.Printf("resource group %q already matches requested values\n", p.Name)
		return nil
	}
	if p.IsDryRun() {
		fmt.Println("Dry run, use --run to save the updated resource group")
		return nil
	}
	fmt.Printf("resource group %q updated: request=%d, limit=%d\n",
		p.Name,
		after.GetConfig().GetRequests().GetNodeNum(),
		after.GetConfig().GetLimits().GetNodeNum(),
	)
	return nil
}

func validateResourceGroupNodeNum(field string, value int64) error {
	if value == resourceGroupNodeNumUnset {
		return nil
	}
	if value < 0 {
		return fmt.Errorf("%s node num must be >= 0 or -1 for unchanged", field)
	}
	if value > math.MaxInt32 {
		return fmt.Errorf("%s node num %d exceeds int32 max", field, value)
	}
	return nil
}

func updateResourceGroupNodeNums(ctx context.Context, cli kv.MetaKV, basePath, name string, request, limit int64, dryRun bool) (*querypb.ResourceGroup, *querypb.ResourceGroup, bool, error) {
	rgs, err := common.ListResourceGroups(ctx, cli, basePath, func(rg *models.ResourceGroup) bool {
		return rg.GetProto().GetName() == name
	})
	if err != nil {
		return nil, nil, false, err
	}
	if len(rgs) == 0 {
		return nil, nil, false, fmt.Errorf("resource group %q not found", name)
	}
	if len(rgs) > 1 {
		return nil, nil, false, fmt.Errorf("found multiple resource groups named %q", name)
	}

	before := rgs[0].GetProto()
	after := proto.Clone(before).(*querypb.ResourceGroup)
	changed := applyResourceGroupNodeNums(after, request, limit)
	if !changed {
		return before, after, false, nil
	}

	fmt.Println("before alter resource group:")
	fmt.Println(before.String())
	fmt.Println("after alter resource group:")
	fmt.Println(after.String())

	if dryRun {
		return before, after, true, nil
	}

	bs, err := proto.Marshal(after)
	if err != nil {
		return nil, nil, false, err
	}
	if err := cli.Save(ctx, rgs[0].Key(), string(bs)); err != nil {
		return nil, nil, false, err
	}
	return before, after, true, nil
}

func applyResourceGroupNodeNums(rg *querypb.ResourceGroup, request, limit int64) bool {
	if rg.Config == nil {
		rg.Config = &rgpb.ResourceGroupConfig{}
	}

	changed := false
	if request != resourceGroupNodeNumUnset {
		if rg.Config.Requests == nil {
			rg.Config.Requests = &rgpb.ResourceGroupLimit{}
		}
		requestNodeNum := int32(request)
		if rg.Config.Requests.NodeNum != requestNodeNum {
			rg.Config.Requests.NodeNum = requestNodeNum
			changed = true
		}
	}
	if limit != resourceGroupNodeNumUnset {
		if rg.Config.Limits == nil {
			rg.Config.Limits = &rgpb.ResourceGroupLimit{}
		}
		limitNodeNum := int32(limit)
		if rg.Config.Limits.NodeNum != limitNodeNum {
			rg.Config.Limits.NodeNum = limitNodeNum
			changed = true
		}
	}
	return changed
}
