package repair

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	etcdversion "github.com/milvus-io/birdwatcher/states/etcd/version"
)

type ChannelWatchedParam struct {
	framework.ParamBase `use:"repair channel-watch"`
	CollectionID        int64  `name:"collection" default:"0" desc:"collection id to repair"`
	ChannelName         string `name:"vchannel" default:"" desc:"channel name to repair"`
	Run                 bool   `name:"run" default:"false" desc:"whether to remove legacy collection meta, default set to \"false\" to dry run"`
}

func (c *ComponentRepair) RepairChannelWatchedCommand(ctx context.Context, p *ChannelWatchedParam) error {
	infos, err := common.ListChannelWatch(ctx, c.client, c.basePath, etcdversion.GetVersion(), func(channel *models.ChannelWatch) bool {
		return (p.CollectionID == 0 || channel.Vchan.CollectionID == p.CollectionID) &&
			(p.ChannelName == "" || channel.Vchan.ChannelName == p.ChannelName)
	})
	if err != nil {
		return errors.Errorf("failed to list channel watch info, %w", err)
	}

	var targets []*models.ChannelWatch

	for _, info := range infos {
		if info.Schema == nil {
			targets = append(targets, info)
		}
	}

	if len(targets) == 0 {
		fmt.Println("No empty schema watch info found")
		return nil
	}

	for _, info := range targets {
		fmt.Println("=================================================================")
		fmt.Printf("Watch info with empty schema found, channel name = %s, key = %s", info.Vchan.ChannelName, info.Key())

		collection, err := common.GetCollectionByIDVersion(ctx, c.client, c.basePath, etcdversion.GetVersion(), info.Vchan.CollectionID)
		if err != nil {
			fmt.Println("failed to get collection schema: ", err.Error())
		}
		sb := &strings.Builder{}
		info.Schema = &collection.Schema
		printSchema(sb, info)
		fmt.Println("Collection schema found, about to set schema as:")
		fmt.Println(sb.String())
		if p.Run {
			err := common.WriteChannelWatchInfo(ctx, c.client, c.basePath, info, collection.CollectionPBv2)
			if err != nil {
				fmt.Println("failed to write modified channel watch info, err: ", err.Error())
				continue
			}
			fmt.Println("Modified channel watch info written!")
		}
	}

	return nil
}

func printSchema(sb *strings.Builder, info *models.ChannelWatch) {
	fmt.Fprintf(sb, "Fields:\n")
	fields := info.Schema.Fields
	sort.Slice(fields, func(i, j int) bool {
		return fields[i].FieldID < fields[j].FieldID
	})
	for _, field := range fields {
		fmt.Fprintf(sb, " - Field ID: %d \t Field Name: %s \t Field Type: %s\n", field.FieldID, field.Name, field.DataType.String())
		if field.IsPrimaryKey {
			fmt.Fprintf(sb, "\t - Primary Key: %t, AutoID: %t\n", field.IsPrimaryKey, field.AutoID)
		}
		if field.IsDynamic {
			fmt.Fprintf(sb, "\t - Dynamic Field\n")
		}
		if field.IsPartitionKey {
			fmt.Fprintf(sb, "\t - Partition Key\n")
		}
		if field.IsClusteringKey {
			fmt.Fprintf(sb, "\t - Clustering Key\n")
		}
		// print element type if field is array
		if field.DataType == models.DataTypeArray {
			fmt.Fprintf(sb, "\t - Element Type:  %s\n", field.ElementType.String())
		}
		// type params
		for key, value := range field.Properties {
			fmt.Fprintf(sb, "\t - Type Param %s: %s\n", key, value)
		}
	}

	fmt.Fprintf(sb, "Enable Dynamic Schema: %t\n", info.Schema.EnableDynamicSchema)
}
