package show

import (
	"bytes"
	"context"
	"fmt"
	"path"
	"sort"
	"strconv"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/spf13/cobra"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/birdwatcher/proto/v2.0/etcdpb"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
)

// CollectionCommand returns sub command for showCmd.
// show collection [options...]
func CollectionCommand(cli *clientv3.Client, basePath string) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "collections",
		Short:   "list current available collection from RootCoord",
		Aliases: []string{"collection"},
		RunE: func(cmd *cobra.Command, args []string) error {
			collectionID, err := cmd.Flags().GetInt64("id")
			if err != nil {
				return err
			}

			var kvs []*mvccpb.KeyValue
			if collectionID > 0 {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
				defer cancel()
				resp, err := cli.Get(ctx, path.Join(basePath, "root-coord/collection", strconv.FormatInt(collectionID, 10)))
				if err != nil {
					return err
				}
				kvs = resp.Kvs
			} else {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
				defer cancel()
				resp, err := cli.Get(ctx, path.Join(basePath, "root-coord/collection"), clientv3.WithPrefix())
				if err != nil {
					return err
				}
				kvs = resp.Kvs
			}

			errors := make(map[string]error)
			for _, kv := range kvs {
				processCollectionKV(cli, basePath, kv, func(key string, err error) {
					errors[key] = err
				})
			}
			for key, err := range errors {
				fmt.Printf("key:%s meet error when trying to parse as Collection: %v\n", key, err)
			}
			return nil
		},
	}

	cmd.Flags().Int64("id", 0, "collection id to display")
	return cmd
}

func processCollectionKV(cli *clientv3.Client, basePath string, kv *mvccpb.KeyValue, handleErr func(key string, err error)) {
	if bytes.Equal(kv.Value, common.CollectionTombstone) {
		return
	}

	collection := &etcdpb.CollectionInfo{}
	err := proto.Unmarshal(kv.Value, collection)
	if err != nil {
		handleErr(string(kv.Key), err)
		return
	}

	err = common.FillFieldSchemaIfEmpty(cli, basePath, collection)
	if err != nil {
		handleErr(string(kv.Key), err)
		return
	}

	printCollection(collection)
}

func printCollection(collection *etcdpb.CollectionInfo) {
	fmt.Println("================================================================================")
	fmt.Printf("Collection ID: %d\tCollection Name: %s\n", collection.ID, collection.Schema.Name)
	fmt.Printf("Partitions:\n")
	for idx, partID := range collection.GetPartitionIDs() {
		fmt.Printf(" - Partition ID: %d\tPartition Name: %s\n", partID, collection.GetPartitionNames()[idx])
	}
	fmt.Printf("Fields:\n")
	fields := collection.Schema.Fields
	sort.Slice(fields, func(i, j int) bool {
		return fields[i].FieldID < fields[j].FieldID
	})
	for _, field := range fields {
		fmt.Printf(" - Field ID: %d \t Field Name: %s \t Field Type: %s\n", field.FieldID, field.Name, field.DataType.String())
		if field.IsPrimaryKey {
			fmt.Printf("\t - Primary Key, AutoID: %v\n", field.AutoID)
		}
		for _, tp := range field.TypeParams {
			fmt.Printf("\t - Type Param %s: %s\n", tp.Key, tp.Value)
		}
	}

	fmt.Printf("Consistency Level: %s\n", collection.GetConsistencyLevel().String())
	for _, startPos := range collection.StartPositions {
		fmt.Printf("Start position for channel %s: %v\n", startPos.Key, startPos.Data)
	}
}
