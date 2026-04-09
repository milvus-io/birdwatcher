package remove

import (
	"path"

	"github.com/milvus-io/birdwatcher/configs"
	"github.com/milvus-io/birdwatcher/states/kv"
)

type ComponentRemove struct {
	client kv.MetaKV
	config *configs.Config
	// basePath is the root path of etcd key-value pairs.
	// by default is by-dev
	basePath string
	// metaPath is the concatenated path of basePath & metaPath
	// by default is by-dev/meta
	metaPath string
}

func NewComponent(cli kv.MetaKV, config *configs.Config, basePath string, metaPart string) *ComponentRemove {
	return &ComponentRemove{
		client:   cli,
		config:   config,
		basePath: basePath,
		metaPath: path.Join(basePath, metaPart),
	}
}
