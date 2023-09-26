package remove

import (
	"context"
	"fmt"
	"path"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/samber/lo"
	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"

	datapbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/datapb"
)

var backupKeyPrefix = "birdwatcher/backup"

// BinlogCommand returns remove binlog file from segment command.
func BinlogCommand(cli clientv3.KV, basePath string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "binlog",
		Short: "Remove binlog file from segment with specified segment id and binlog key",
		Run: func(cmd *cobra.Command, args []string) {
			logType, err := cmd.Flags().GetString("logType")
			if err != nil {
				fmt.Println(err.Error())
				return
			}

			collectionID, err := cmd.Flags().GetInt64("collectionID")
			if err != nil {
				fmt.Println(err.Error())
				return
			}

			partitionID, err := cmd.Flags().GetInt64("partitionID")
			if err != nil {
				fmt.Println(err.Error())
				return
			}

			segmentID, err := cmd.Flags().GetInt64("segmentID")
			if err != nil {
				fmt.Println(err.Error())
				return
			}

			fieldID, err := cmd.Flags().GetInt64("fieldID")
			if err != nil {
				fmt.Println(err.Error())
				return
			}

			logID, err := cmd.Flags().GetInt64("logID")
			if err != nil {
				fmt.Println(err.Error())
				return
			}

			var key string
			switch logType {
			case "binlog":
				key = path.Join(basePath, "datacoord-meta",
					fmt.Sprintf("binlog/%d/%d/%d/%d", collectionID, partitionID, segmentID, fieldID))
			case "deltalog":
				key = path.Join(basePath, "datacoord-meta",
					fmt.Sprintf("deltalog/%d/%d/%d/%d", collectionID, partitionID, segmentID, fieldID))
			case "statslog":
				key = path.Join(basePath, "datacoord-meta",
					fmt.Sprintf("statslog/%d/%d/%d/%d", collectionID, partitionID, segmentID, fieldID))
			default:
				fmt.Println("logType unknown:", logType)
				return
			}

			restore, err := cmd.Flags().GetBool("restore")
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			if restore {
				err = restoreBinlog(cli, key)
				if err != nil {
					fmt.Println(err.Error())
				}
				return
			}

			err = backupBinlog(cli, key)
			if err != nil {
				fmt.Println(err.Error())
				return
			}

			run, err := cmd.Flags().GetBool("run")
			if err != nil {
				fmt.Println(err.Error())
				return
			}

			removeAll, err := cmd.Flags().GetBool("removeAll")
			if err != nil {
				fmt.Println(err.Error())
				return
			}

			// remove all
			if removeAll {
				_, err = getFieldBinlog(cli, key)
				if err != nil {
					fmt.Println(err.Error())
					return
				}
				if !run {
					return
				}
				fmt.Printf("key:%s will be deleted\n", key)
				err = removeBinlog(cli, key)
				if err != nil {
					fmt.Println(err.Error())
					return
				}
				return
			}

			// remove one
			{
				fieldBinlog, err := getFieldBinlog(cli, key)
				if err != nil {
					fmt.Println(err.Error())
					return
				}
				fieldBinlog, err = removeLogFromFieldBinlog(key, logID, fieldBinlog)
				if err != nil {
					fmt.Println(err.Error())
					return
				}

				if !run {
					return
				}

				err = saveFieldBinlog(cli, key, fieldBinlog)
				if err != nil {
					fmt.Println(err.Error())
					return
				}
				fmt.Printf("Remove one binlog %s/%d from etcd succeeds.\n", key, logID)
			}

		},
	}

	cmd.Flags().String("logType", "unknown", "log type: binlog/deltalog/statslog")
	cmd.Flags().Bool("run", false, "flags indicating whether to execute removed command")
	cmd.Flags().Bool("restore", false, "flags indicating whether to restore removed command")
	cmd.Flags().Bool("removeAll", false, "remove all binlogs belongs to the field")
	cmd.Flags().Int64("collectionID", 0, "collection id to remove")
	cmd.Flags().Int64("partitionID", 0, "partition id to remove")
	cmd.Flags().Int64("segmentID", 0, "segment id to remove")
	cmd.Flags().Int64("fieldID", 0, "field id to remove")
	cmd.Flags().Int64("logID", 0, "log id to remove")
	return cmd
}

func backupBinlog(cli clientv3.KV, key string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	resp, err := cli.Get(ctx, key)
	if err != nil {
		fmt.Printf("get key:%s failed\n", key)
		return err
	}

	for _, kv := range resp.Kvs {
		backupKey := path.Join(backupKeyPrefix, string(kv.Key))
		fmt.Printf("start backup key:%s to %s \n", key, backupKey)
		_, err = cli.Put(ctx, backupKey, string(kv.Value))
		if err != nil {
			fmt.Println("failed save kv into etcd, ", err.Error())
			return err
		}
	}
	fmt.Printf("backup key:%s finished\n", key)
	return nil
}

func restoreBinlog(cli clientv3.KV, key string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	backupKey := path.Join(backupKeyPrefix, key)
	resp, err := cli.Get(ctx, backupKey)
	if err != nil {
		fmt.Printf("get backup key:%s failed\n", backupKey)
		return err
	}

	for _, kv := range resp.Kvs {
		fmt.Printf("start restore key:%s to %s\n", backupKey, key)
		_, err = cli.Put(ctx, key, string(kv.Value))
		if err != nil {
			fmt.Println("failed save kv into etcd, ", err.Error())
			return err
		}
	}
	fmt.Printf("restore key:%s finished\n", key)
	return nil
}

func removeBinlog(cli clientv3.KV, key string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	_, err := cli.Delete(ctx, key)
	if err != nil {
		fmt.Printf("delete key:%s failed\n", key)
		return err
	}
	fmt.Printf("remove key:%s finished\n", key)
	return nil
}

func getFieldBinlog(cli clientv3.KV, key string) (*datapbv2.FieldBinlog, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	resp, err := cli.Get(ctx, key)
	if err != nil {
		fmt.Printf("get key:%s failed\n", key)
		return nil, err
	}
	fieldBinlog := &datapbv2.FieldBinlog{}
	err = proto.Unmarshal(resp.Kvs[0].Value, fieldBinlog)
	if err != nil {
		return nil, err
	}
	fmt.Println("FieldBinlog(before):")
	fmt.Println("**************************************")
	fmt.Println(fieldBinlog)
	fmt.Println("**************************************")
	return fieldBinlog, nil
}

func removeLogFromFieldBinlog(key string, logID int64, fieldBinlog *datapbv2.FieldBinlog) (*datapbv2.FieldBinlog, error) {
	binlogs := lo.Filter(fieldBinlog.GetBinlogs(), func(binlog *datapbv2.Binlog, _ int) bool {
		if logID == binlog.GetLogID() {
			fmt.Printf("logID matched, binlog: %s/%d\n", key, logID)
		}
		return logID != binlog.GetLogID()
	})
	fieldBinlog.Binlogs = binlogs

	fmt.Println("FieldBinlog(after):")
	fmt.Println("**************************************")
	fmt.Println(fieldBinlog)
	fmt.Println("**************************************")
	return fieldBinlog, nil
}

func saveFieldBinlog(cli clientv3.KV, key string, fieldBinlog *datapbv2.FieldBinlog) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	mb, err := proto.Marshal(fieldBinlog)
	if err != nil {
		return err
	}
	_, err = cli.Put(ctx, key, string(mb))
	if err != nil {
		fmt.Println("failed save field binlog kv into etcd, ", err.Error())
		return err
	}
	fmt.Printf("save field binlog kv done. key: %s\n", key)
	return nil
}
