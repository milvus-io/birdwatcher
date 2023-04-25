package states

import (
	"fmt"
	"os"
	"path"

	"github.com/milvus-io/birdwatcher/configs"
	"github.com/milvus-io/birdwatcher/states/etcd"
	"github.com/milvus-io/birdwatcher/states/etcd/audit"
	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// instanceState provides command for single milvus instance.
type instanceState struct {
	cmdState
	instanceName string
	client       clientv3.KV
	auditFile    *os.File

	etcdState State
	config    *configs.Config
}

func (s *instanceState) Close() {
	if s.auditFile != nil {
		s.auditFile.Close()
	}
}

// SetupCommands setups the command.
// also called after each command run to reset flag values.
func (s *instanceState) SetupCommands() {
	cmd := &cobra.Command{}

	cli := s.client
	instanceName := s.instanceName

	basePath := path.Join(instanceName, metaPath)

	showCmd := etcd.ShowCommand(cli, basePath)
	showCmd.AddCommand(
		// show current-version
		CurrentVersionCommand(),
		// show segment-loaded-grpc
		GetDistributionCommand(cli, basePath),
		// show configurations
		GetConfigurationCommand(cli, basePath),
	)

	cmd.AddCommand(
		// download-segment
		getDownloadSegmentCmd(cli, basePath),
		// show [subcommand] options...
		showCmd,
		// repair [subcommand] options...
		etcd.RepairCommand(cli, basePath),
		// remove [subcommand] options...
		etcd.RemoveCommand(cli, basePath),
		// set [subcommand] options...
		etcd.SetCommand(cli, instanceName, metaPath),

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

		// segment-loaded
		GetDistributionCommand(cli, basePath),

		// probe
		GetProbeCmd(cli, basePath),

		// set current-version
		SetCurrentVersionCommand(),

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
		getDisconnectCmd(s, s.config),
		// exit
		getExitCmd(s),
	)

	s.cmdState.rootCmd = cmd
	s.setupFn = s.SetupCommands
}

// getDryModeCmd enter dry-mode
func getDryModeCmd(cli clientv3.KV, state *instanceState, etcdState State) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "dry-mode",
		Short: "enter dry mode to select instance",
		Run: func(*cobra.Command, []string) {
			state.SetNext(etcdState)
		},
	}
	return cmd
}

func getInstanceState(cli clientv3.KV, instanceName string, etcdState State, config *configs.Config) State {

	var kv clientv3.KV
	file, err := os.OpenFile("audit.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, os.ModeAppend)
	if err != nil {
		fmt.Println("failed to open audit.log file!")
		kv = cli
	} else {
		kv = audit.NewFileAuditKV(cli, file)
	}
	// use audit kv
	state := &instanceState{
		cmdState: cmdState{
			label: fmt.Sprintf("Milvus(%s)", instanceName),
		},
		instanceName: instanceName,
		client:       kv,
		auditFile:    file,

		etcdState: etcdState,
		config:    config,
	}

	state.SetupCommands()

	return state
}
