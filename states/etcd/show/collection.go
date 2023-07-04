package show

import (
	"context"
	"fmt"
	"sort"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	etcdversion "github.com/milvus-io/birdwatcher/states/etcd/version"
	"github.com/milvus-io/birdwatcher/utils"
)

// CollectionCommand returns sub command for showCmd.
// show collection [options...]
type CollectionParam struct {
	framework.ParamBase `use:"show collections" desc:"list current available collection from RootCoord"`
	CollectionID        int64  `name:"id" default:"0" desc:"collection id to display"`
	CollectionName      string `name:"name" default:"" desc:"collection name to display"`
	DatabaseID          int64  `name:"dbid" default:"-1" desc:"database id to filter"`
}

func (c *ComponentShow) CollectionCommand(ctx context.Context, p *CollectionParam) error {
	var collections []*models.Collection
	var total int64
	var err error
	// perform get by id to accelerate
	if p.CollectionID > 0 {
		var collection *models.Collection
		collection, err = common.GetCollectionByIDVersion(ctx, c.client, c.basePath, etcdversion.GetVersion(), p.CollectionID)
		if err == nil {
			collections = append(collections, collection)
		}
	} else {
		collections, err = common.ListCollectionsVersion(ctx, c.client, c.basePath, etcdversion.GetVersion(), func(coll *models.Collection) bool {
			if p.CollectionName != "" && coll.Schema.Name != p.CollectionName {
				return false
			}
			if p.DatabaseID > -1 && coll.DBID != p.DatabaseID {
				return false
			}
			total++
			return true
		})
	}

	if err != nil {
		return err
	}
	channels := 0
	healthy := 0
	for _, collection := range collections {
		printCollection(collection)
		if collection.State == models.CollectionStateCollectionCreated {
			channels += len(collection.Channels)
			healthy++
		}
	}
	fmt.Println("================================================================================")
	fmt.Printf("--- Total collections:  %d\t Matched collections:  %d\n", total, len(collections))
	fmt.Printf("--- Total channel: %d\t Healthy collections: %d\n", channels, healthy)

	return nil
}

func printCollection(collection *models.Collection) {
	fmt.Println("================================================================================")
	fmt.Printf("DBID: %d\n", collection.DBID)
	fmt.Printf("Collection ID: %d\tCollection Name: %s\n", collection.ID, collection.Schema.Name)
	t, _ := utils.ParseTS(collection.CreateTime)
	fmt.Printf("Collection State: %s\tCreate Time: %s\n", collection.State.String(), t.Format("2006-01-02 15:04:05"))
	/*
		fmt.Printf("Partitions:\n")
		for idx, partID := range collection.GetPartitionIDs() {
			fmt.Printf(" - Partition ID: %d\tPartition Name: %s\n", partID, collection.GetPartitionNames()[idx])
		}*/
	fmt.Printf("Fields:\n")
	fields := collection.Schema.Fields
	sort.Slice(fields, func(i, j int) bool {
		return fields[i].FieldID < fields[j].FieldID
	})
	for _, field := range fields {
		fmt.Printf(" - Field ID: %d \t Field Name: %s \t Field Type: %s\n", field.FieldID, field.Name, field.DataType.String())
		if field.IsPrimaryKey {
			fmt.Printf("\t - Primary Key: %t, AutoID: %t\n", field.IsPrimaryKey, field.AutoID)
		}
		if field.IsDynamic {
			fmt.Printf("\t - Dynamic Field\n")
		}
		if field.IsPartitionKey {
			fmt.Printf("\t - Partition Key\n")
		}
		for key, value := range field.Properties {
			fmt.Printf("\t - Type Param %s: %s\n", key, value)
		}
	}

	fmt.Printf("Enable Dynamic Schema: %t\n", collection.Schema.EnableDynamicSchema)
	fmt.Printf("Consistency Level: %s\n", collection.ConsistencyLevel.String())
	for _, channel := range collection.Channels {
		fmt.Printf("Start position for channel %s(%s): %v\n", channel.PhysicalName, channel.VirtualName, channel.StartPosition.MsgID)
	}
}
