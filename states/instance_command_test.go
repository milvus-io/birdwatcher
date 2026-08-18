package states

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/birdwatcher/configs"
	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/states/etcd/set"
	"github.com/milvus-io/birdwatcher/states/etcd/show"
)

func TestInstanceStateRegistersSetResourceGroupCommand(t *testing.T) {
	state := &InstanceState{
		CmdState:      framework.NewCmdState("test", &configs.Config{}),
		ComponentShow: show.NewComponent(nil, nil, "by-dev", "meta"),
		ComponentSet:  set.NewComponent(nil, nil, "by-dev/meta"),
		instanceName:  "by-dev",
		metaPath:      "meta",
		basePath:      "by-dev/meta",
	}

	state.SetupCommands()

	cmd, _, err := state.RootCmd.Find([]string{"set", "resource-group"})
	require.NoError(t, err)
	require.NotNil(t, cmd)
	require.Equal(t, "resource-group", cmd.Use)
}
