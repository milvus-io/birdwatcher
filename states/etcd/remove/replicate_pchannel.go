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
	framework.ExecutionParam `use:"remove replicate-pchannel" desc:"Remove dirty replicate pchannel meta not in current config topology"`
	TargetCluster            string `name:"targetCluster" default:"" desc:"target cluster id to filter"`
	SourceChannel            string `name:"sourceChannel" default:"" desc:"source channel name to filter"`
}

// RemoveReplicatePChannelCommand defines `remove replicate-pchannel` command.
// It only allows removing pchannel metas whose target cluster is NOT in the
// current replicate configuration topology (i.e. dirty/stale entries).
func (c *ComponentRemove) RemoveReplicatePChannelCommand(ctx context.Context, p *RemoveReplicatePChannelParam) error {
	if p.TargetCluster == "" || p.SourceChannel == "" {
		fmt.Println("both --targetCluster and --sourceChannel are required")
		fmt.Println("use 'show replicate' to list all replicate pchannel metas first")
		return nil
	}

	// Build valid target cluster set from replicate configuration
	validTargets := make(map[string]struct{})
	cfg, err := common.ListReplicateConfiguration(ctx, c.client, c.metaPath)
	if err != nil && err != common.ErrReplicateConfigurationNotFound {
		return err
	}
	if cfg != nil {
		for _, cluster := range cfg.GetReplicateConfiguration().GetClusters() {
			validTargets[cluster.GetClusterId()] = struct{}{}
		}
	}

	metas, keys, err := common.ListReplicatePChannelWithKeys(ctx, c.client, c.metaPath)
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
	t.AppendHeader(table.Row{"#", "SourcePChannel", "TargetCluster", "TargetPChannel", "InConfig", "Selected"})

	var targets []string
	var skippedInConfig int
	for i, meta := range metas {
		targetClusterID := meta.GetTargetCluster().GetClusterId()

		// Check if this target cluster is in the active config topology
		_, inConfig := validTargets[targetClusterID]

		// Apply user filters
		matchFilter := true
		if !strings.Contains(targetClusterID, p.TargetCluster) {
			matchFilter = false
		}
		if !strings.Contains(meta.GetSourceChannelName(), p.SourceChannel) {
			matchFilter = false
		}

		selected := matchFilter && !inConfig
		if matchFilter && inConfig {
			skippedInConfig++
		}

		marker := ""
		if selected {
			marker = "Y"
			targets = append(targets, keys[i])
		}

		inConfigStr := "N"
		if inConfig {
			inConfigStr = "Y"
		}

		t.AppendRow(table.Row{
			i,
			meta.GetSourceChannelName(),
			targetClusterID,
			meta.GetTargetChannelName(),
			inConfigStr,
			marker,
		})
	}
	t.Render()

	if skippedInConfig > 0 {
		fmt.Printf("\nError: %d matched meta(s) belong to an active target cluster in the current config, refusing to delete\n", skippedInConfig)
		fmt.Println("only dirty/stale replicate pchannel metas (target cluster not in config) can be removed")
		return nil
	}

	if len(targets) == 0 {
		fmt.Println("no dirty replicate pchannel meta matched the filter")
		return nil
	}

	fmt.Printf("\n%d dirty replicate pchannel meta(s) selected for removal\n", len(targets))

	if !p.Run {
		return nil
	}

	fmt.Println("Start to delete dirty replicate pchannel meta...")
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
