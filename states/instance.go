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
	cmd := &cobra.Command{}

	state := &instanceState{
		cmdState: cmdState{
			label:   fmt.Sprintf("Milvus(%s)", instanceName),
			rootCmd: cmd,
		},
		instanceName: instanceName,
		client:       cli,
	}

	cmd.AddCommand(
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
		getVisitCmd(state, cli, path.Join(instanceName, metaPath)),
		// show-log-level
		getShowLogLevelCmd(cli, path.Join(instanceName, metaPath)),
		// update-log-level log_level_name component serverId
		getUpdateLogLevelCmd(cli, path.Join(instanceName, metaPath)),
		// clean-empty-segment
		cleanEmptySegments(cli, path.Join(instanceName, metaPath)),
		// clean-empty-segment-by-id
		cleanEmptySegmentByID(cli, path.Join(instanceName, metaPath)),
		// garbage-collect
		getGarbageCollectCmd(cli, path.Join(instanceName, metaPath)),
		// dry-mode
		getDryModeCmd(cli, state, etcdState),
		// disconnect
		getDisconnectCmd(state),
		// exit
		getExitCmd(state),
	)

	return state
}
