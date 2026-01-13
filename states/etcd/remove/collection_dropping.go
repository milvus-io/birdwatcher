package remove

import (
	"context"
	"fmt"
	"path"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/milvus/pkg/v2/proto/etcdpb"
)

type CollectionDropParam struct {
	framework.ParamBase `use:"remove collection-drop" desc:"Remove collection & channel meta for collection in dropping/dropped state"`
	CollectionID        int64 `name:"collectionID" default:"0" desc:"collection id to remove"`
	Run                 bool  `name:"run" default:"false" desc:"flags indicating whether to execute removed command"`
}

func (c *ComponentRemove) CollectionDropCommand(ctx context.Context, p *CollectionDropParam) error {
	var collections []*models.Collection
	var err error
	if p.CollectionID > 0 {
		collection, err := common.GetCollectionByIDVersion(ctx, c.client, c.basePath, p.CollectionID)
		if err != nil {
			fmt.Printf("failed to get collection by id(%d): %s\n", p.CollectionID, err.Error())
			return err
		}
		// skip healthy collection
		if collection.GetProto().State != etcdpb.CollectionState_CollectionDropping && collection.GetProto().State != etcdpb.CollectionState_CollectionDropped {
			fmt.Printf("Collection State is [%s]\n", collection.GetProto().State.String())
			return err
		}
		collections = append(collections, collection)
	} else {
		collections, err = common.ListCollections(context.Background(), c.client, c.basePath, func(coll *models.Collection) bool {
			return coll.GetProto().State == etcdpb.CollectionState_CollectionDropping || coll.GetProto().State == etcdpb.CollectionState_CollectionDropped
		})
		if err != nil {
			fmt.Println("failed to list collection", err.Error())
			return err
		}
	}

	for _, collection := range collections {
		fmt.Printf("Found Dropping Collection ID: %s[%d]\n", collection.GetProto().Schema.Name, collection.GetProto().ID)
		err := c.cleanCollectionMeta(ctx, collection, p.Run)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *ComponentRemove) cleanCollectionMeta(ctx context.Context, info *models.Collection, run bool) error {
	collection := info.GetProto()
	fmt.Println("Clean collection(drop) meta:")
	if info.Key() == "" {
		return fmt.Errorf("Collection %s[%d] key is empty string, cannot perform cleanup", collection.Schema.Name, collection.ID)
	}
	basePath := c.basePath
	cli := c.client

	// TODO: alias meta can't be cleaned conveniently.
	prefixes := []string{
		// remove collection field meta
		path.Join(basePath, fmt.Sprintf("root-coord/fields/%d", collection.ID)) + "/",
		path.Join(basePath, common.SnapshotPrefix, fmt.Sprintf("root-coord/fields/%d", collection.ID)) + "/",

		// remove collection partition meta
		path.Join(basePath, fmt.Sprintf("root-coord/partitions/%d", collection.ID)) + "/",
		path.Join(basePath, common.SnapshotPrefix, fmt.Sprintf("root-coord/partitions/%d", collection.ID)) + "/",
	}
	keys := []string{info.Key()}
	var collectionKey string

	if collection.DbId != 0 {
		collectionKey = fmt.Sprintf("root-coord/database/collection-info/%d/%d", collection.DbId, collection.ID)
	} else {
		collectionKey = fmt.Sprintf("root-coord/collection/%d", collection.ID)
	}

	// collection will have timestamp suffix, which should be also removed by prefix.
	prefixes = append(prefixes, path.Join(basePath, common.SnapshotPrefix, collectionKey))

	channelWatchInfos, err := common.ListChannelWatch(context.Background(), cli, basePath, func(cw *models.ChannelWatch) bool {
		return cw.GetProto().Vchan.CollectionID == collection.ID
	})
	if err != nil {
		return fmt.Errorf("failed to list channel watch info for collection[%d], err: %s\n", collection.ID, err.Error())
	}
	for _, info := range channelWatchInfos {
		// remove channel watch meta
		if info.Key() == "" {
			return fmt.Errorf("channel[%s] watch info key is empty", info.GetProto().Vchan.ChannelName)
		}
		fmt.Println("channel watch info:", info.Key())
		keys = append(keys, info.Key())
	}

	// channel checkpoint and removal
	for _, channel := range info.Channels() {
		cpKey := path.Join(basePath, "datacoord-meta/channel-cp", channel.VirtualName)
		fmt.Println("channel checkpoint:", cpKey)
		keys = append(keys, cpKey)
		removalKey := path.Join(basePath, "datacoord-meta/channel-removal", channel.VirtualName)
		fmt.Println("channel removal", removalKey)
		keys = append(keys, removalKey)
	}

	// dry run
	if !run {
		fmt.Println("Dry run complete")
		return nil
	}

	for _, prefix := range prefixes {
		if err := cli.RemoveWithPrefix(ctx, prefix); err != nil {
			fmt.Printf("failed to clean prefix: %s, error: %s\n", prefix, err.Error())
		} else {
			fmt.Printf("clean prefix: %s\n", prefix)
		}
	}

	for _, key := range keys {
		if err := cli.Remove(ctx, key); err != nil {
			fmt.Printf("failed to clean key: %s, error: %s\n", key, err.Error())
		} else {
			fmt.Printf("clean key: %s\n", key)
		}
	}

	return nil
}
