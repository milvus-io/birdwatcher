package remove

import (
	"context"
	"fmt"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
)

type RemoveChannelParam struct {
	framework.ParamBase `use:"remove channel" desc:"Remove channel from datacoord meta with specified condition if orphan"`

	Channel string `name:"channel" default:"" desc:"channel name to remove"`
	Run     bool   `name:"run" default:"false" desc:"flags indicating whether to remove channel from meta, default is false"`
	Force   bool   `name:"force" default:"false" desc:"force remove channel ignoring collection check"`
}

// RemoveChannelCommand defines `remove channel` command.
func (c *ComponentRemove) RemoveChannelCommand(ctx context.Context, p *RemoveChannelParam) error {
	collections, err := common.ListCollections(ctx, c.client, c.basePath)
	if err != nil {
		return err
	}

	validChannels := make(map[string]struct{})
	for _, collection := range collections {
		for _, channel := range collection.GetProto().GetVirtualChannelNames() {
			validChannels[channel] = struct{}{}
		}
	}

	watchChannels, err := common.ListChannelWatch(ctx, c.client, c.basePath, func(info *models.ChannelWatch) bool {
		if len(p.Channel) > 0 {
			return info.GetProto().GetVchan().GetChannelName() == p.Channel
		}
		return true
	})

	if err != nil {
		return err
	}

	cps, err := common.ListChannelCheckpoint(ctx, c.client, c.basePath, func(pos *models.MsgPosition) bool {
		if len(p.Channel) > 0 {
			return pos.GetProto().GetChannelName() == p.Channel
		}
		return true
	})

	if err != nil {
		return err
	}

	var targets []string
	for _, watchChannel := range watchChannels {
		_, ok := validChannels[watchChannel.GetProto().GetVchan().GetChannelName()]
		if !ok || p.Force {
			fmt.Printf("%s selected as target channel, collection id: %d\n", watchChannel.GetProto().GetVchan().GetChannelName(), watchChannel.GetProto().GetVchan().GetCollectionID())
			targets = append(targets, watchChannel.Key())
		}
	}

	for _, cp := range cps {
		_, ok := validChannels[cp.GetProto().GetChannelName()]
		if !ok || p.Force {
			fmt.Printf("%s selected as target orpah checkpoint\n", cp.GetProto().GetChannelName())
			targets = append(targets, cp.Key())
		}
	}

	if !p.Run {
		return nil
	}
	fmt.Printf("Start to delete orphan watch channel info...\n")
	for _, path := range targets {
		err := c.client.Remove(ctx, path)
		if err != nil {
			fmt.Printf("failed to remove watch key %s, error: %s\n", path, err.Error())
			continue
		}
		fmt.Printf("remove orphan channel %s done\n", path)
	}
	return nil
}
