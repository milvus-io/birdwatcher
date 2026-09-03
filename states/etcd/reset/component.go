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
	client kv.MetaKV
	config *configs.Config
	// basePath is {instanceName}/{metaPath}: where Milvus keeps its own metadata.
	basePath string
	// instanceName is the etcd root path. Woodpecker roots its metadata at
	// {instanceName}/{meta-prefix}, i.e. a sibling of Milvus's meta path rather than a
	// child of it, so `reset wal` needs the instance name and not just basePath.
	instanceName string
}

func NewComponent(cli kv.MetaKV, config *configs.Config, basePath, instanceName string) *ComponentReset {
	return &ComponentReset{
		client:       cli,
		config:       config,
		basePath:     basePath,
		instanceName: instanceName,
	}
}
