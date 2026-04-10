package remove

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/jedib0t/go-pretty/v6/table"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
)

type RemoveReplicatePChannelParam struct {
	framework.ExecutionParam `use:"remove replicate-pchannel" desc:"Remove replicate pchannel meta from streaming coord"`
	TargetCluster            string `name:"targetCluster" default:"" desc:"target cluster id to filter"`
	SourceChannel            string `name:"sourceChannel" default:"" desc:"source channel name to filter"`
}

// RemoveReplicatePChannelCommand defines `remove replicate-pchannel` command.
func (c *ComponentRemove) RemoveReplicatePChannelCommand(ctx context.Context, p *RemoveReplicatePChannelParam) error {
	metas, keys, err := common.ListReplicatePChannelWithKeys(ctx, c.client, c.basePath)
	if err != nil {
		return err
	}

	if len(metas) == 0 {
		fmt.Println("no replicate pchannel meta found")
		return nil
	}

	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)
	t.SetTitle("Replicate PChannel Metas")
	t.AppendHeader(table.Row{"#", "SourcePChannel", "TargetCluster", "TargetPChannel", "Key", "Selected"})

	var targets []string
	for i, meta := range metas {
		selected := true
		if p.TargetCluster != "" && !strings.Contains(meta.GetTargetCluster().GetClusterId(), p.TargetCluster) {
			selected = false
		}
		if p.SourceChannel != "" && !strings.Contains(meta.GetSourceChannelName(), p.SourceChannel) {
			selected = false
		}

		marker := ""
		if selected {
			marker = "Y"
			targets = append(targets, keys[i])
		}
		t.AppendRow(table.Row{
			i,
			meta.GetSourceChannelName(),
			meta.GetTargetCluster().GetClusterId(),
			meta.GetTargetChannelName(),
			keys[i],
			marker,
		})
	}
	t.Render()

	if len(targets) == 0 {
		fmt.Println("no replicate pchannel meta matched the filter")
		return nil
	}

	fmt.Printf("\n%d replicate pchannel meta(s) selected for removal\n", len(targets))

	if !p.Run {
		return nil
	}

	fmt.Println("Start to delete replicate pchannel meta...")
	for _, key := range targets {
		err := c.client.Remove(ctx, key)
		if err != nil {
			fmt.Printf("failed to remove key %s, error: %s\n", key, err.Error())
			continue
		}
		fmt.Printf("removed replicate pchannel meta: %s\n", key)
	}
	fmt.Printf("Done. Removed %d replicate pchannel meta(s)\n", len(targets))
	return nil
}
