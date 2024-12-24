package states

import (
	"github.com/milvus-io/birdwatcher/common"
	"github.com/milvus-io/birdwatcher/configs"
	"github.com/milvus-io/birdwatcher/models"
	etcdversion "github.com/milvus-io/birdwatcher/states/etcd/version"
)

// Start returns the first state - offline.
func Start(config *configs.Config) common.State {
	app := &ApplicationState{
		State:  getDisconnectedState(config),
		config: config,
	}

	etcdversion.SetVersion(models.GTEVersion2_2)

	return app
}

// ApplicationState application background state.
// used for state switch/merging.
type ApplicationState struct {
	// current state
	common.State

	// config stores configuration items
	config *configs.Config
}
