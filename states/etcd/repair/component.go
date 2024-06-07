package repair

import (
	"github.com/milvus-io/birdwatcher/configs"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type ComponentRepair struct {
	client   clientv3.KV
	config   *configs.Config
	basePath string
}

func NewComponent(cli clientv3.KV, config *configs.Config, basePath string) *ComponentRepair {
	return &ComponentRepair{
		client:   cli,
		config:   config,
		basePath: basePath,
	}
}
