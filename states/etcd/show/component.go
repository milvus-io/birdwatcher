package show

import (
	"github.com/milvus-io/birdwatcher/configs"
	"github.com/milvus-io/birdwatcher/states/kv"
)

type ComponentShow struct {
	client   kv.MetaKV
	config   *configs.Config
	basePath string
}

func NewComponent(cli kv.MetaKV, config *configs.Config, basePath string) *ComponentShow {
	return &ComponentShow{
		client:   cli,
		config:   config,
		basePath: basePath,
	}
}
