package states

import (
	"fmt"
	"path"

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

	cmd.AddCommand(
		// download-segment
		getDownloadSegmentCmd(cli, path.Join(instanceName, metaPath)),
		// show [subcommand] options...
		getEtcdShowCmd(cli, path.Join(instanceName, metaPath)),
		// backup [component]
		getBackupEtcdCmd(cli, path.Join(instanceName, metaPath)),
		// kill --component [component] --id [id]
		getEtcdKillCmd(cli, path.Join(instanceName, metaPath)),
		// force-release
		getForceReleaseCmd(cli, path.Join(instanceName, metaPath)),
		// download-pk
		getDownloadPKCmd(cli, path.Join(instanceName, metaPath)),
		// visit [component] [id]
		getVisitCmd(s, cli, path.Join(instanceName, metaPath)),
		// show-log-level
		getShowLogLevelCmd(cli, path.Join(instanceName, metaPath)),
		// update-log-level log_level_name component serverId
		getUpdateLogLevelCmd(cli, path.Join(instanceName, metaPath)),
		// clean-empty-segment
		cleanEmptySegments(cli, path.Join(instanceName, metaPath)),
		// remove-segment-by-id
		removeSegmentByID(cli, path.Join(instanceName, metaPath)),
		// garbage-collect
		getGarbageCollectCmd(cli, path.Join(instanceName, metaPath)),
		// release-dropped-collection
		getReleaseDroppedCollectionCmd(cli, path.Join(instanceName, metaPath)),
		// repair-segment
		getRepairSegmentCmd(cli, path.Join(instanceName, metaPath)),
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
