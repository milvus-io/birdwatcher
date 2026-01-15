package show

import (
	"path"

	"github.com/milvus-io/birdwatcher/configs"
	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/states/kv"
)

type ComponentShow struct {
	client kv.MetaKV
	config *configs.Config
	// basePath is the root path of etcd key-value pairs.
	// by default is by-dev
	basePath string
	// metaPath is the concatenated path of basePath & metaPath
	// by default is by-dev/meta
	metaPath string
}

func NewComponent(cli kv.MetaKV, config *configs.Config, basePath string, metaPath string) *ComponentShow {
	return &ComponentShow{
		client:   cli,
		config:   config,
		basePath: basePath,
		metaPath: path.Join(basePath, metaPath),
	}
}

// GetGlobalFormat implements framework.FormatProvider interface.
func (c *ComponentShow) GetGlobalFormat() framework.Format {
	if c.config != nil {
		formatName := c.config.GetGlobalOutputFormat()
		if formatName != "" {
			return framework.NameFormat(formatName)
		}
	}
	return framework.FormatDefault
}
