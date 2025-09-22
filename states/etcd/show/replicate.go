package show

import (
	"context"
	"fmt"
	"os"

	"github.com/jedib0t/go-pretty/v6/table"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/milvus/pkg/v2/util/replicateutil"
)

type ReplicateParam struct {
	framework.ParamBase `use:"show replicate" desc:"list current replicate configuration of milvus"`
}

func (c *ComponentShow) ReplicateCommand(ctx context.Context, p *ReplicateParam) error {
	cfg, err := common.ListReplicateConfiguration(ctx, c.client, c.metaPath)
	if err != nil {
		return err
	}
	cfgHelper, err := replicateutil.NewConfigHelper(c.basePath, cfg.ReplicateConfiguration)
	if err != nil {
		return err
	}
	cdcTasks, err := common.ListReplicatePChannel(ctx, c.client, c.metaPath)
	if err != nil {
		return err
	}
	cfgJSON, _ := protojson.Marshal(cfgHelper.GetReplicateConfiguration())
	fmt.Println(string(cfgJSON))
	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)
	t.SetTitle("Replicate CDC Tasks")
	t.AppendHeader(table.Row{"SourcePChannel", "TargetCluster", "TargetPChannel", "InitializedMessageID", "InitializedTimeTick"})
	for _, cdcTask := range cdcTasks {
		t.AppendRow(table.Row{
			cdcTask.SourceChannelName,
			cdcTask.TargetCluster.ClusterId,
			cdcTask.TargetChannelName,
			common.GetMessageIDString("", cdcTask.InitializedCheckpoint.MessageId.Id),
			cdcTask.InitializedCheckpoint.TimeTick,
		})
	}
	t.Render()
	return nil
}
