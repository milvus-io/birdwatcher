package remove

import (
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/birdwatcher/configs"
)

type ComponentRemove struct {
	client   clientv3.KV
	config   *configs.Config
	basePath string
}

func NewComponent(cli clientv3.KV, config *configs.Config, basePath string) *ComponentRemove {
	return &ComponentRemove{
		client:   cli,
		config:   config,
		basePath: basePath,
	}
}
