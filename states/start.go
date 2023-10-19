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
	minioTag  = "minio"
)

// Start returns the first state - offline.
func Start(config *configs.Config) framework.State {
	app := &ApplicationState{
		states: map[string]framework.State{},
		config: config,
	}

	app.core = framework.NewCmdState("Offline")
	app.SetupCommands()

	etcdversion.SetVersion(models.GTEVersion2_2)

	return app
}
