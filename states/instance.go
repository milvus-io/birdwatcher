package states

import (
	"fmt"
	"path"

	"github.com/milvus-io/birdwatcher/states/etcd"
	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// instanceState provides command for single milvus instance.
type instanceState struct {
	cmdState
	instanceName string
	client       *clientv3.Client

	etcdState State
}

// SetupCommands setups the command.
// also called after each command run to reset flag values.
func (s *instanceState) SetupCommands() {
	cmd := &cobra.Command{}

	cli := s.client
	instanceName := s.instanceName

	basePath := path.Join(instanceName, metaPath)

	cmd.AddCommand(
		// download-segment
		getDownloadSegmentCmd(cli, basePath),
		// show [subcommand] options...
		etcd.ShowCommand(cli, basePath),
		// repair [subcommand] options...
		etcd.RepairCommand(cli, basePath),
		// backup [component]
		getBackupEtcdCmd(cli, basePath),
		// kill --component [component] --id [id]
		getEtcdKillCmd(cli, basePath),
		// force-release
		getForceReleaseCmd(cli, basePath),
		// download-pk
		getDownloadPKCmd(cli, basePath),
		// visit [component] [id]
		getVisitCmd(s, cli, basePath),
		// show-log-level
		getShowLogLevelCmd(cli, basePath),
		// update-log-level log_level_name component serverId
		getUpdateLogLevelCmd(cli, basePath),

		// remove-segment-by-id
		//removeSegmentByID(cli, basePath),
		// garbage-collect
		getGarbageCollectCmd(cli, basePath),
		// release-dropped-collection
		getReleaseDroppedCollectionCmd(cli, basePath),

		// fetch-metrics
		getFetchMetricsCmd(cli, basePath),
		// dry-mode
		getDryModeCmd(cli, s, s.etcdState),
		// disconnect
		getDisconnectCmd(s),
		// exit
		getExitCmd(s),
	)

	s.cmdState.rootCmd = cmd
	s.setupFn = s.SetupCommands
}

// getDryModeCmd enter dry-mode
func getDryModeCmd(cli *clientv3.Client, state *instanceState, etcdState State) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "dry-mode",
		Short: "enter dry mode to select instance",
		Run: func(*cobra.Command, []string) {
			state.SetNext(etcdState)
		},
	}
	return cmd
}

func getInstanceState(cli *clientv3.Client, instanceName string, etcdState State) State {

	state := &instanceState{
		cmdState: cmdState{
			label: fmt.Sprintf("Milvus(%s)", instanceName),
		},
		instanceName: instanceName,
		client:       cli,

		etcdState: etcdState,
	}

	state.SetupCommands()

	return state
}
