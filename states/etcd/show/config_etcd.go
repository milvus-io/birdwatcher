package show

import (
	"context"
	"fmt"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
)

type ConfigEtcdParam struct {
	framework.ParamBase `use:"show config-etcd" desc:"list configuations set by etcd source"`
}

// ConfigEtcdCommand return show config-etcd command.
func (c *ComponentShow) ConfigEtcdCommand(ctx context.Context, p *ConfigEtcdParam) {
	keys, values, err := common.ListEtcdConfigs(ctx, c.client, c.basePath)
	if err != nil {
		fmt.Println("failed to list configurations from etcd", err.Error())
		return
	}

	for i, key := range keys {
		fmt.Printf("Key: %s, Value: %s\n", key, values[i])
	}
}
