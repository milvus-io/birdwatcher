package show

import (
	"path"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/birdwatcher/configs"
)

type ComponentShow struct {
	client clientv3.KV
	config *configs.Config
	// basePath is the root path of etcd key-value pairs.
	// by default is by-dev
	basePath string
	// metaPath is the concatenated path of basePath & metaPath
	// by default is by-dev/meta
	metaPath string
}

func NewComponent(cli clientv3.KV, config *configs.Config, basePath string, metaPath string) *ComponentShow {
	return &ComponentShow{
		client:   cli,
		config:   config,
		basePath: basePath,
		metaPath: path.Join(basePath, metaPath),
	}
}
