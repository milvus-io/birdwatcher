package states

import (
	"context"
	"fmt"
	"os"
	"path"

	"github.com/milvus-io/birdwatcher/configs"
	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/states/etcd"
	"github.com/milvus-io/birdwatcher/states/etcd/audit"
	"github.com/milvus-io/birdwatcher/states/etcd/remove"
	"github.com/milvus-io/birdwatcher/states/etcd/show"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// InstanceState provides command for single milvus instance.
type InstanceState struct {
	*framework.CmdState
	*show.ComponentShow
	*remove.ComponentRemove
	instanceName string
	client       clientv3.KV
	auditFile    *os.File

	etcdState framework.State
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
	cmd := s.GetCmd()

	cli := s.client
	instanceName := s.instanceName

	basePath := s.basePath

	showCmd := etcd.ShowCommand(cli, basePath)
	showCmd.AddCommand(
		// show segment-loaded-grpc
		GetDistributionCommand(cli, basePath),
	)

	s.MergeCobraCommands(cmd,
		// download-segment
		getDownloadSegmentCmd(cli, basePath),
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

		//balance-explain
		ExplainBalanceCommand(cli, basePath),

		//
		getVerifySegmentCmd(cli, basePath),

		// probe
		GetProbeCmd(cli, basePath),

		// remove-segment-by-id
		//removeSegmentByID(cli, basePath),
		// garbage-collect
		getGarbageCollectCmd(cli, basePath),
		// release-dropped-collection
		getReleaseDroppedCollectionCmd(cli, basePath),

		// web
		getWebCmd(s, cli, basePath),
		// fetch-metrics
		getFetchMetricsCmd(cli, basePath),
	)

	//cmd.AddCommand(etcd.RawCommands(cli)...)

	s.UpdateState(cmd, s, s.SetupCommands)
}

type DryModeParam struct {
	framework.ParamBase `use:"dry-mode" desc:"enter dry mode to select instance"`
}

// DryModeCommand implement `dry-mode` command to enter etcd "dry mode".
func (s *InstanceState) DryModeCommand(ctx context.Context, p *DryModeParam) {
	s.SetNext(s.etcdState)
}

func getInstanceState(parent *framework.CmdState, cli clientv3.KV, instanceName, metaPath string, etcdState framework.State, config *configs.Config) framework.State {
	var kv clientv3.KV
	file, err := os.OpenFile("audit.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, os.ModeAppend)
	if err != nil {
		fmt.Println("failed to open audit.log file!")
		kv = cli
	} else {
		kv = audit.NewFileAuditKV(cli, file)
	}

	basePath := path.Join(instanceName, metaPath)

	// use audit kv
	state := &InstanceState{
		CmdState:        parent.Spawn(fmt.Sprintf("Milvus(%s)", instanceName)),
		ComponentShow:   show.NewComponent(cli, config, basePath),
		ComponentRemove: remove.NewComponent(cli, config, basePath),
		instanceName:    instanceName,
		client:          kv,
		auditFile:       file,

		etcdState: etcdState,
		config:    config,
		basePath:  basePath,
	}

	return state
}
