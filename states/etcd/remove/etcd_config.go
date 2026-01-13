package remove

import (
	"context"
	"fmt"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
)

type RemoveEtcdConfigParam struct {
	framework.ParamBase `use:"remove etcd-config" desc:"remove etcd stored configuations"`
	Key                 string `name:"key" desc:"etcd config key" default:""`
	Run                 bool   `name:"run" default:"false" desc:"flag to control actually run or dry"`
}

func (c *ComponentRemove) RemoveEtcdConfigCommand(ctx context.Context, p *RemoveEtcdConfigParam) error {
	if p.Key == "" {
		fmt.Println("key & value cannot be empty")
		return nil
	}

	value, err := common.GetEtcdConfig(ctx, c.client, c.basePath, p.Key)
	if err != nil {
		return fmt.Errorf("failed to get etcd config item with key %s, %s", p.Key, err.Error())
	}

	if !p.Run {
		fmt.Printf("dry run key: %s value: %s\n", p.Key, value)
		return nil
	}

	fmt.Printf("remove key: %s value: %s\n", p.Key, value)

	return common.RemoveEtcdConfig(ctx, c.client, c.basePath, p.Key)
}
