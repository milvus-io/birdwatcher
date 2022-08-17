package states

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path"
	"sort"
	"strconv"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/birdwatcher/proto/v2.0/etcdpb"
	"github.com/milvus-io/birdwatcher/proto/v2.0/schemapb"
	"github.com/spf13/cobra"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// getEtcdShowCollection returns sub command for showCmd
// show collection [options...]
func getEtcdShowCollection(cli *clientv3.Client, basePath string) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "collection",
		Short:   "list current available collection from RootCoord",
		Aliases: []string{"collections"},
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

func getCollectionByID(cli *clientv3.Client, basePath string, collID int64) (*etcdpb.CollectionInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	resp, err := cli.Get(ctx, path.Join(basePath, "root-coord/collection", strconv.FormatInt(collID, 10)))

	if err != nil {
		return nil, err
	}

	if len(resp.Kvs) != 1 {
		return nil, errors.New("invalid collection id")
	}

	coll := &etcdpb.CollectionInfo{}

	err = proto.Unmarshal(resp.Kvs[0].Value, coll)
	if err != nil {
		return nil, err
	}

	err = fillFieldSchemaIfEmpty(cli, basePath, coll)
	if err != nil {
		return nil, err
	}

	return coll, nil
}

var CollectionTombstone = []byte{0xE2, 0x9B, 0xBC}

func processCollectionKV(cli *clientv3.Client, basePath string, kv *mvccpb.KeyValue, handleErr func(key string, err error)) {
	if bytes.Equal(kv.Value, CollectionTombstone) {
		return
	}

	collection := &etcdpb.CollectionInfo{}
	err := proto.Unmarshal(kv.Value, collection)
	if err != nil {
		handleErr(string(kv.Key), err)
		return
	}

	err = fillFieldSchemaIfEmpty(cli, basePath, collection)
	if err != nil {
		handleErr(string(kv.Key), err)
		return
	}

	printCollection(collection)
}

func fillFieldSchemaIfEmpty(cli *clientv3.Client, basePath string, collection *etcdpb.CollectionInfo) error {
	if len(collection.GetSchema().GetFields()) == 0 { // fields separated from schema after 2.1.1
		resp, err := cli.Get(context.TODO(), path.Join(basePath, fmt.Sprintf("root-coord/fields/%d", collection.ID)), clientv3.WithPrefix())
		if err != nil {
			return err
		}
		for _, kv := range resp.Kvs {
			field := &schemapb.FieldSchema{}
			err := proto.Unmarshal(kv.Value, field)
			if err != nil {
				fmt.Println("found error field:", string(kv.Key), err.Error())
				continue
			}
			collection.Schema.Fields = append(collection.Schema.Fields, field)
		}
	}

	return nil
}

func printCollection(collection *etcdpb.CollectionInfo) {
	fmt.Println("================================================================================")
	fmt.Printf("Collection ID: %d\tCollection Name:%s\n", collection.ID, collection.Schema.Name)
	fmt.Printf("Partitions:\n")
	for idx, partID := range collection.GetPartitionIDs() {
		fmt.Printf(" - Partition ID:%d\tPartition Name:%s\n", partID, collection.GetPartitionNames()[idx])
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
