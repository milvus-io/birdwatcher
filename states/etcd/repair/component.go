package repair

import (
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/birdwatcher/configs"
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
