package set

import (
	"github.com/milvus-io/birdwatcher/configs"
	"github.com/milvus-io/birdwatcher/states/kv"
)

type ComponentSet struct {
	client   kv.MetaKV
	config   *configs.Config
	basePath string
}

func NewComponent(cli kv.MetaKV, config *configs.Config, basePath string) *ComponentSet {
	return &ComponentSet{
		client:   cli,
		config:   config,
		basePath: basePath,
	}
}
