package show

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/fatih/color"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/birdwatcher/utils"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/etcdpb"
)

// CollectionCommand returns sub command for showCmd.
// show collection [options...]
type CollectionParam struct {
	framework.ParamBase `use:"show collections" desc:"list current available collection from RootCoord"`
	CollectionID        int64  `name:"id" default:"0" desc:"collection id to display"`
	CollectionName      string `name:"name" default:"" desc:"collection name to display"`
	DatabaseID          int64  `name:"dbid" default:"-1" desc:"database id to filter"`
	State               string `name:"state" default:"" desc:"collection state to filter"`
	WithPropertyKey     string `name:"propertyKey" default:"" desc:"collection property to filter"`
	Format              string `name:"format" default:"" desc:"output format"`
}

func (c *ComponentShow) CollectionCommand(ctx context.Context, p *CollectionParam) (*framework.PresetResultSet, error) {
	var collections []*models.Collection
	var total int64
	var err error
	// perform get by id to accelerate
	if p.CollectionID > 0 {
		var collection *models.Collection
		collection, err = common.GetCollectionByIDVersion(ctx, c.client, c.metaPath, p.CollectionID)
		if err == nil {
			collections = append(collections, collection)
		}
	} else {
		collections, err = common.ListCollections(ctx, c.client, c.metaPath, func(info *models.Collection) bool {
			coll := info.GetProto()
			if p.CollectionName != "" && coll.Schema.Name != p.CollectionName {
				return false
			}
			if p.DatabaseID > -1 && coll.DbId != p.DatabaseID {
				return false
			}
			if p.State != "" && !strings.EqualFold(p.State, coll.State.String()) {
				return false
			}

			if p.WithPropertyKey != "" {
				found := false
				for _, prop := range coll.Properties {
					if prop.Key == p.WithPropertyKey {
						found = true
						break
					}
				}
				if !found {
					return false
				}
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
		if collection.GetProto().State == etcdpb.CollectionState_CollectionCreated {
			channels += len(collection.GetProto().GetVirtualChannelNames())
			healthy++
		}
	}

	return framework.NewPresetResultSet(&Collections{
		collections: collections,
		total:       total,
		channels:    channels,
		healthy:     healthy,
	}, framework.NameFormat(p.Format)), nil
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
		fmt.Fprintf(sb, "--- Total collections:  %d\t Matched collections:  %d\n", rs.total, len(rs.collections))
		fmt.Fprintf(sb, "--- Total channel: %d\t Healthy collections: %d\n", rs.channels, rs.healthy)
		return sb.String()
	case framework.FormatLine:
		sb := &strings.Builder{}
		for _, coll := range rs.collections {
			fmt.Fprintf(sb, "collection id %d\t collection name %s\n", coll.GetProto().ID, coll.GetProto().Schema.Name)
		}
		return sb.String()
	}
	return ""
}

func (rs *Collections) Entities() any {
	return rs.collections
}

func printCollection(sb *strings.Builder, info *models.Collection) {
	collection := info.GetProto()
	fmt.Fprintln(sb, "================================================================================")
	fmt.Fprintf(sb, "DBID: %d\n", collection.DbId)
	fmt.Fprintf(sb, "Collection ID: %d\tCollection Name: %s\n", collection.ID, collection.Schema.Name)
	createTime, _ := utils.ParseTS(collection.CreateTime)
	updateTime, _ := utils.ParseTS(collection.UpdateTimestamp)
	fmt.Fprintf(sb, "Collection State: %s\tCreate Time: %s\n", collection.State.String(), createTime.Format("2006-01-02 15:04:05"))
	fmt.Fprintf(sb, "Update Time: %s\tUpdate timestamp: %d\n", updateTime.Format("2006-01-02 15:04:05"), collection.UpdateTimestamp)
	fmt.Fprintf(sb, "Fields:\n")
	fields := collection.Schema.Fields
	sort.Slice(fields, func(i, j int) bool {
		return fields[i].FieldID < fields[j].FieldID
	})
	for _, field := range fields {
		fmt.Fprintf(sb, " - Field ID: %d \t Field Name: %s \t Field Type: %s\n", field.FieldID, field.Name, field.DataType.String())
		if field.IsPrimaryKey {
			fmt.Fprintf(sb, "\t - Primary Key: %t, AutoID: %t\n", field.IsPrimaryKey, field.AutoID)
		}
		if field.GetNullable() {
			fmt.Fprintf(sb, "\t - %s\n", color.MagentaString("Nullable"))
		}
		if field.GetDefaultValue() != nil {
			fmt.Fprintf(sb, "\t - %s: %v\n", color.MagentaString("DefaultValue"), field.GetDefaultValue())
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
		if field.IsFunctionOutput {
			fmt.Fprintf(sb, "\t - Function Output\n")
		}
		// print element type if field is array
		if field.DataType == schemapb.DataType_Array {
			fmt.Fprintf(sb, "\t - Element Type:  %s\n", field.ElementType.String())
		}
		// type params
		for _, kv := range field.TypeParams {
			fmt.Fprintf(sb, "\t - Type Param %s: %s\n", kv.Key, kv.Value)
		}
	}
	for _, structField := range info.GetProto().GetSchema().GetStructArrayFields() {
		fmt.Fprintf(sb, " - Struct Field ID: %d \t Field Name: %s\n", structField.FieldID, structField.Name)
		for _, field := range structField.Fields {
			fmt.Fprintf(sb, "\t - Field ID: %d \t Field Name: %s \t Field Type: %s\n", field.FieldID, field.Name, field.DataType.String())
		}
	}

	fmt.Fprintf(sb, "Enable Dynamic Schema: %t\n", collection.Schema.EnableDynamicField)
	for _, function := range info.Functions {
		fp := function.GetProto()
		fmt.Fprintf(sb, "Function Name: %s, Type: %s, Input: %v, Output: %v\n", fp.GetName(), fp.GetType().String(), fp.GetInputFieldNames(), fp.GetOutputFieldNames())
	}
	fmt.Fprintf(sb, "Consistency Level: %s\n", collection.ConsistencyLevel.String())
	for _, channel := range info.Channels() {
		fmt.Fprintf(sb, "Start position for channel %s(%s): %v\n", channel.PhysicalName, channel.VirtualName, channel.StartPosition.MsgID)
	}
	fmt.Fprintf(sb, "Collection properties(%d):\n", len(collection.Properties))
	for _, kv := range collection.Properties {
		fmt.Fprintf(sb, "\tKey: %s: %v\n", kv.GetKey(), kv.GetValue())
	}
	fmt.Fprintf(sb, "Shard Infos(%d):\n", len(collection.ShardInfos))
	for idx, shardInfo := range collection.ShardInfos {
		fmt.Fprintf(sb, "VChannelName: %s, LastTruncateTimeTick: %d\n", collection.VirtualChannelNames[idx], shardInfo.LastTruncateTimeTick)
	}
}
