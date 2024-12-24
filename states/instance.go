package states

import (
	"fmt"
	"os"
	"path"
	"time"

	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/birdwatcher/common"
	"github.com/milvus-io/birdwatcher/configs"
	"github.com/milvus-io/birdwatcher/states/etcd"
	"github.com/milvus-io/birdwatcher/states/etcd/audit"
	"github.com/milvus-io/birdwatcher/states/etcd/remove"
	"github.com/milvus-io/birdwatcher/states/etcd/repair"
	"github.com/milvus-io/birdwatcher/states/etcd/set"
	"github.com/milvus-io/birdwatcher/states/etcd/show"
)

// InstanceState provides command for single milvus instance.
type InstanceState struct {
	common.CmdState
	*show.ComponentShow
	*remove.ComponentRemove
	*repair.ComponentRepair
	*set.ComponentSet
	instanceName string
	client       clientv3.KV
	auditFile    *os.File

	etcdState common.State
	config    *configs.Config
	basePath  string
}

func (s *InstanceState) Close() {
	if s.auditFile != nil {
		s.auditFile.Close()
	}
}

// SetupCommands setups the command.
// also called after each command run to reset flag values.
func (s *InstanceState) SetupCommands() {
	cmd := &cobra.Command{}

	cli := s.client
	instanceName := s.instanceName

	basePath := s.basePath

	showCmd := etcd.ShowCommand(cli, basePath)
	showCmd.AddCommand(
		// show current-version
		CurrentVersionCommand(),
	)

	cmd.AddCommand(
		// show [subcommand] options...
		showCmd,
		// repair [subcommand] options...
		etcd.RepairCommand(cli, basePath),
		// remove [subcommand] options...
		etcd.RemoveCommand(cli, instanceName, basePath),
		// set [subcommand] options...
		etcd.SetCommand(cli, instanceName, metaPath),
		// restore [subcommand] options...
		// etcd.RestoreCommand(cli, basePath),

		// backup [component]
		getBackupEtcdCmd(cli, basePath),
		// kill --component [component] --id [id]
		getEtcdKillCmd(cli, basePath),
		// visit [component] [id]
		getVisitCmd(s, cli, basePath),
		// show-log-level
		getShowLogLevelCmd(cli, basePath),
		// update-log-level log_level_name component serverId
		getUpdateLogLevelCmd(cli, basePath),

		// balance-explain
		ExplainBalanceCommand(cli, basePath),

		//
		getVerifySegmentCmd(cli, basePath),

		// probe
		GetProbeCmd(cli, basePath),

		// remove-segment-by-id
		// removeSegmentByID(cli, basePath),
		// garbage-collect
		getGarbageCollectCmd(cli, basePath),
		// release-dropped-collection
		getReleaseDroppedCollectionCmd(cli, basePath),

		// web
		getWebCmd(s, cli, basePath),
		// fetch-metrics
		getFetchMetricsCmd(cli, basePath),
		// dry-mode
		getDryModeCmd(s, s.etcdState),
		etcd.DownloadCommand(cli, basePath),
	)

	// cmd.AddCommand(etcd.RawCommands(cli)...)
	s.MergeFunctionCommands(cmd, s)
	s.CmdState.RootCmd = cmd
	s.SetupFn = s.SetupCommands
}

// getDryModeCmd enter dry-mode
func getDryModeCmd(state *InstanceState, etcdState common.State) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "dry-mode",
		Short: "enter dry mode to select instance",
		Run: func(*cobra.Command, []string) {
			state.SetNext(etcdState)
		},
	}
	return cmd
}

func getInstanceState(cli clientv3.KV, instanceName, metaPath string, etcdState common.State, config *configs.Config) common.State {
	var kv clientv3.KV
	name := fmt.Sprintf("audit_%s.log", time.Now().Format("2006_0102_150405"))
	file, err := os.OpenFile(name, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		fmt.Println("failed to open audit.log file!")
		kv = cli
	} else {
		kv = audit.NewFileAuditKV(cli, file)
	}

	basePath := path.Join(instanceName, metaPath)

	// use audit kv
	state := &InstanceState{
		CmdState: common.CmdState{
			LabelStr: fmt.Sprintf("Milvus(%s)", instanceName),
		},
		ComponentShow:   show.NewComponent(cli, config, basePath),
		ComponentRemove: remove.NewComponent(cli, config, basePath),
		ComponentRepair: repair.NewComponent(cli, config, basePath),
		ComponentSet:    set.NewComponent(cli, config, basePath),
		instanceName:    instanceName,
		client:          kv,
		auditFile:       file,

		etcdState: etcdState,
		config:    config,
		basePath:  basePath,
	}

	state.SetupCommands()

	return state
}
