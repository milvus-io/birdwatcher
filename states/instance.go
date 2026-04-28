package states

import (
	"context"
	"fmt"
	"os"
	"path"
	"time"

	"github.com/spf13/cobra"

	"github.com/milvus-io/birdwatcher/configs"
	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/states/etcd"
	"github.com/milvus-io/birdwatcher/states/etcd/remove"
	"github.com/milvus-io/birdwatcher/states/etcd/repair"
	"github.com/milvus-io/birdwatcher/states/etcd/set"
	"github.com/milvus-io/birdwatcher/states/etcd/show"
	metakv "github.com/milvus-io/birdwatcher/states/kv"
)

// InstanceState provides command for single milvus instance.
type InstanceState struct {
	*framework.CmdState
	*show.ComponentShow
	*remove.ComponentRemove
	*repair.ComponentRepair
	*set.ComponentSet
	instanceName string
	metaPath     string
	client       metakv.MetaKV
	auditFile    *os.File

	etcdState  framework.State
	config     *configs.Config
	basePath   string
	extensions []Extension
}

func (s *InstanceState) Close() {
	if s.auditFile != nil {
		s.auditFile.Close()
	}
}

// GetGlobalFormat implements framework.FormatProvider interface.
func (s *InstanceState) GetGlobalFormat() framework.Format {
	if s.config != nil {
		formatName := s.config.GetGlobalOutputFormat()
		if formatName != "" {
			return framework.NameFormat(formatName)
		}
	}
	return framework.FormatDefault
}

func (s *InstanceState) InstanceName() string { return s.instanceName }

func (s *InstanceState) MetaPath() string { return s.metaPath }

func (s *InstanceState) BasePath() string { return s.basePath }

func (s *InstanceState) Client() metakv.MetaKV { return s.client }

func (s *InstanceState) Config() *configs.Config { return s.config }

func (s *InstanceState) State() framework.State { return s }

// SetupCommands setups the command.
// also called after each command run to reset flag values.
func (s *InstanceState) SetupCommands() {
	cmd := s.GetCmd()

	cli := s.client
	instanceName := s.instanceName
	metaPath := s.metaPath

	basePath := s.basePath

	showCmd := etcd.ShowCommand(cli, basePath)

	s.MergeCobraCommands(cmd,
		// show [subcommand] options...
		showCmd,
		// repair [subcommand] options...
		etcd.RepairCommand(cli, basePath),
		// set [subcommand] options...
		etcd.SetCommand(cli, instanceName, metaPath),
		// restore [subcommand] options...
		// etcd.RestoreCommand(cli, basePath),

		// visit [component] [id]
		getVisitCmd(s.CmdState, cli, basePath),
		// show-log-level
		getShowLogLevelCmd(cli, basePath),
		// update-log-level log_level_name component serverId
		getUpdateLogLevelCmd(cli, basePath),

		// balance-explain
		ExplainBalanceCommand(cli, basePath),

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
		etcd.DownloadCommand(cli, basePath),
	)

	s.mergeExtensionCommands(cmd)

	// cmd.AddCommand(etcd.RawCommands(cli)...)

	s.UpdateState(cmd, s, s.SetupCommands)
}

func (s *InstanceState) mergeExtensionCommands(cmd *cobra.Command) {
	for _, ext := range s.extensions {
		if provider, ok := ext.(InstanceCobraCommandProvider); ok {
			s.MergeCobraCommands(cmd, provider.InstanceCobraCommands(s)...)
		}
		if provider, ok := ext.(InstanceFunctionCommandProvider); ok {
			for _, receiver := range provider.InstanceFunctionCommands(s) {
				s.MergeFunctionCommandsFrom(cmd, s, receiver)
			}
		}
	}
}

type DryModeParam struct {
	framework.ParamBase `use:"dry-mode" desc:"enter dry mode to select instance"`
}

// DryModeCommand implement `dry-mode` command to enter etcd "dry mode".
func (s *InstanceState) DryModeCommand(ctx context.Context, p *DryModeParam) {
	s.SetNext(etcdTag, s.etcdState)
}

func getInstanceState(parent *framework.CmdState, cli metakv.MetaKV, instanceName, metaPath string, etcdState framework.State, config *configs.Config, extensions []Extension) framework.State {
	var kv metakv.MetaKV
	name := fmt.Sprintf("audit_%s.log", time.Now().Format("2006_0102_150405"))
	file, err := os.OpenFile(name, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		fmt.Println("failed to open audit.log file!")
		kv = cli
	} else {
		kv = metakv.NewFileAuditKV(cli, file)
	}

	basePath := path.Join(instanceName, metaPath)

	// use audit kv
	state := &InstanceState{
		CmdState:        parent.Spawn(fmt.Sprintf("Milvus(%s)", instanceName)),
		ComponentShow:   show.NewComponent(cli, config, instanceName, metaPath),
		ComponentRemove: remove.NewComponent(cli, config, instanceName, metaPath),
		ComponentRepair: repair.NewComponent(cli, config, basePath),
		ComponentSet:    set.NewComponent(cli, config, basePath),
		instanceName:    instanceName,
		metaPath:        metaPath,
		client:          kv,
		auditFile:       file,

		etcdState:  etcdState,
		config:     config,
		basePath:   basePath,
		extensions: extensions,
	}

	return state
}
