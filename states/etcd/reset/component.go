package reset

import (
	"github.com/milvus-io/birdwatcher/configs"
	"github.com/milvus-io/birdwatcher/states/kv"
)

// ComponentReset hosts the `reset` command family.
//
// Unlike `repair`, which fixes metadata that is broken, `reset` deliberately
// discards metadata that is still valid — so every command here requires the
// Milvus cluster to be stopped first.
type ComponentReset struct {
	client   kv.MetaKV
	config   *configs.Config
	basePath string
}

func NewComponent(cli kv.MetaKV, config *configs.Config, basePath string) *ComponentReset {
	return &ComponentReset{
		client:   cli,
		config:   config,
		basePath: basePath,
	}
}
