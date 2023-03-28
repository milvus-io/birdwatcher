package states

import (
	"github.com/milvus-io/birdwatcher/configs"
	"github.com/milvus-io/birdwatcher/models"
	etcdversion "github.com/milvus-io/birdwatcher/states/etcd/version"
	"github.com/spf13/cobra"
)

// Start returns the first state - offline.
func Start(config *configs.Config) State {
	root := &cobra.Command{
		Use:   "",
		Short: "",
	}

	state := &cmdState{
		label:   "Offline",
		rootCmd: root,
	}
	app := &ApplicationState{
		State: state,
	}

	app.config = config

	etcdversion.SetVersion(models.GTEVersion2_2)

	root.AddCommand(
		// connect
		getConnectCommand(state, app.config),
		// load-backup
		getLoadBackupCmd(state, app.config),
		// open-workspace
		getOpenWorkspaceCmd(state, app.config),
		// pulsarctl
		getPulsarctlCmd(state),
		// parse-indexparams
		GetParseIndexParamCmd(),
		// assemble-indexfiles
		GetAssembleIndexFilesCmd(),
		// validate-indexfiles
		GetValidateIndexFilesCmd(),
		// exit
		getExitCmd(state))

	root.AddCommand(getGlobalUtilCommands()...)

	return app
}

// ApplicationState application background state.
// used for state switch/merging.
type ApplicationState struct {
	// current state
	State

	// config stores configuration items
	config *configs.Config
}
