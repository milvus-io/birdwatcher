package repair

import (
	"github.com/milvus-io/birdwatcher/configs"
	"github.com/milvus-io/birdwatcher/states/kv"
)

type ComponentRepair struct {
	client   kv.MetaKV
	config   *configs.Config
	basePath string
}

func NewComponent(cli kv.MetaKV, config *configs.Config, basePath string) *ComponentRepair {
	return &ComponentRepair{
		client:   cli,
		config:   config,
		basePath: basePath,
	}
}
