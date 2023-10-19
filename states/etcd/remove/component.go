package remove

import (
	"github.com/milvus-io/birdwatcher/configs"
	"github.com/milvus-io/birdwatcher/states/kv"
)

type ComponentRemove struct {
	client   kv.MetaKV
	config   *configs.Config
	basePath string
}

func NewComponent(cli kv.MetaKV, config *configs.Config, basePath string) *ComponentRemove {
	return &ComponentRemove{
		client:   cli,
		config:   config,
		basePath: basePath,
	}
}
