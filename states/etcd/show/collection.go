package show

import (
	"context"
	"fmt"
	"sort"
	"strings"

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
	State               string `name:"state" default:"" desc:"collection state to filter"`
}

func (c *ComponentShow) CollectionCommand(ctx context.Context, p *CollectionParam) (*Collections, error) {
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
			if p.State != "" && !strings.EqualFold(p.State, coll.State.String()) {
				return false
			}

			total++
			return true
		})
	}

	if err != nil {
		return nil, err
	}
	channels := 0
	healthy := 0
	for _, collection := range collections {
		if collection.State == models.CollectionStateCollectionCreated {
			channels += len(collection.Channels)
			healthy++
		}
	}

	return &Collections{
		collections: collections,
		total:       total,
		channels:    channels,
		healthy:     healthy,
	}, nil
}

type Collections struct {
	collections []*models.Collection
	total       int64
	channels    int
	healthy     int
}

func (rs *Collections) PrintAs(format framework.Format) string {
	switch format {
	case framework.FormatDefault, framework.FormatPlain:
		sb := &strings.Builder{}
		for _, coll := range rs.collections {
			printCollection(sb, coll)
		}
		fmt.Fprintln(sb, "================================================================================")
		fmt.Printf("--- Total collections:  %d\t Matched collections:  %d\n", rs.total, len(rs.collections))
		fmt.Printf("--- Total channel: %d\t Healthy collections: %d\n", rs.channels, rs.healthy)
		return sb.String()
	}
	return ""
}

func (rs *Collections) Entities() any {
	return rs.collections
}

func printCollection(sb *strings.Builder, collection *models.Collection) {
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
	fmt.Printf("Collection properties(%d):", len(collection.Properties))
	for k, v := range collection.Properties {
		fmt.Printf("\tKey: %s: %v\n", k, v)
	}
}
