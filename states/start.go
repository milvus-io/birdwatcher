package states

import (
	"github.com/milvus-io/birdwatcher/configs"
	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	etcdversion "github.com/milvus-io/birdwatcher/states/etcd/version"
)

const (
	etcdTag   = "etcd"
	tikvTag   = "tikv"
	pulsarTag = "pulsar"
	ossTag    = "oss"
)

// Start returns the first state - offline.
func Start(config *configs.Config, multiStage bool, opts ...Option) framework.State {
	app := &ApplicationState{
		states: map[string]framework.State{},
		config: config,
	}

	for _, opt := range opts {
		if opt != nil {
			opt(app)
		}
	}

	app.core = framework.NewCmdState("[core]", config)
	app.SetupCommands()

	etcdversion.SetVersion(models.GTEVersion2_2)

	return app
}
