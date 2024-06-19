package show

import (
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/birdwatcher/configs"
)

type ComponentShow struct {
	client   clientv3.KV
	config   *configs.Config
	basePath string
}

func NewComponent(cli clientv3.KV, config *configs.Config, basePath string) *ComponentShow {
	return &ComponentShow{
		client:   cli,
		config:   config,
		basePath: basePath,
	}
}
