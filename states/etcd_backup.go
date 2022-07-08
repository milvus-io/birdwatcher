package states

import (
	"bufio"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"strings"
	"time"

	"github.com/congqixia/birdwatcher/models"
	"github.com/congqixia/birdwatcher/proto/v2.0/commonpb"
	"github.com/golang/protobuf/proto"
	"github.com/gosuri/uilive"
	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type milvusComponent string

const (
	compAll        milvusComponent = "ALL"
	compQueryCoord milvusComponent = "QUERYCOORD"
	compRootCoord  milvusComponent = "ROOTCOORD"
	compDataCoord  milvusComponent = "DATACOORD"
	compIndexCoord milvusComponent = "INDEXCOORD"

	compQueryNode milvusComponent = "QUERYNODE"
)

// String implements pflag.Value.
func (c *milvusComponent) String() string {
	return string(*c)
}

// Set implements pflag.Value.
func (c *milvusComponent) Set(v string) error {
	switch strings.ToUpper(v) {
	case string(compAll), string(compQueryCoord), string(compRootCoord), string(compDataCoord), string(compIndexCoord),
		string(compQueryNode):
		*c = milvusComponent(strings.ToUpper(v))
	default:
		return errors.New(`must be one of "ALL", "QueryCoord", "DataCoord", "IndexCoord" or "RootCoord"`)
	}
	return nil
}

// Type implements pflag.Value.
func (c *milvusComponent) Type() string {
	return "MilvusComponent"
}

// getBackupEtcdCmd returns command for backup etcd
// usage: backup [component] [options...]
func getBackupEtcdCmd(cli *clientv3.Client, basePath string) *cobra.Command {

	component := compAll
	cmd := &cobra.Command{
		Use:   "backup",
		Short: "backup etcd key-values",
		RunE: func(cmd *cobra.Command, args []string) error {
			prefix := ""

			switch component {
			case compAll:
				prefix = ""
			case compQueryCoord:
				prefix = `queryCoord-`
			default:
				return fmt.Errorf("component %s not support yet", component)
			}

			now := time.Now()
			err := backupEtcd(cli, basePath, prefix, component.String(), fmt.Sprintf("bw_etcd_%s.%s.bak.gz", component, now.Format("060102-150405")))
			if err != nil {
				fmt.Printf("backup etcd failed, error: %v\n", err)
			}
			return nil
		},
	}

	cmd.Flags().Var(&component, "component", "component to backup")
	return cmd
}

// backupEtcd backup all key-values with prefix provided into local file.
// implements gzip compression for now.
func backupEtcd(cli *clientv3.Client, base, prefix string, component string, filePath string) error {
	resp, err := cli.Get(context.Background(), path.Join(base, prefix), clientv3.WithCountOnly(), clientv3.WithPrefix())
	if err != nil {
		return err
	}

	cnt := resp.Count
	rev := resp.Header.Revision

	fmt.Printf("found %d keys, at revision %d, starting backup...\n", cnt, rev)

	f, err := os.OpenFile(filePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer f.Close()

	gw := gzip.NewWriter(f)
	defer gw.Close()
	w := bufio.NewWriter(gw)

	var instance, meta string
	parts := strings.Split(prefix, "/")
	if len(parts) > 1 {
		meta = parts[len(parts)-1]
		instance = path.Join(parts[:len(parts)-1]...)
	} else {
		instance = prefix
	}

	header := &models.BackupHeader{Version: 1, Instance: instance, MetaPath: meta, Entries: cnt}
	bs, err := proto.Marshal(header)
	if err != nil {
		fmt.Println("failed to marshal backup header,", err.Error())
		return err
	}
	w.Write(bs)
	w.WriteString("\n")

	progressDisplay := uilive.New()
	progressFmt := "Backing up ... %d%%(%d/%d)\n"
	progressDisplay.Start()
	fmt.Fprintf(progressDisplay, progressFmt, 0, 0, cnt)

	currentKey := path.Join(base, prefix)
	for i := 0; int64(i) < cnt; i++ {
		resp, err = cli.Get(context.Background(), currentKey, clientv3.WithRev(rev), clientv3.WithFromKey(), clientv3.WithLimit(1))
		if err != nil {
			return err
		}

		for _, kvs := range resp.Kvs {

			entry := &commonpb.KeyDataPair{Key: string(kvs.Key), Data: kvs.Value}
			bs, err = proto.Marshal(entry)
			if err != nil {
				fmt.Println("failed to marsahl kv pair", err.Error())
				return err
			}

			w.Write(bs)
			w.WriteString("\n")
			currentKey = string(append(kvs.Key, 0))
		}

		progress := (i + 1) * 100 / int(cnt)
		fmt.Fprintf(progressDisplay, progressFmt, progress, i+1, cnt)
	}
	w.Flush()
	progressDisplay.Stop()

	fmt.Printf("backup etcd for prefix %s done, stored in file: %s\n", prefix, filePath)

	return nil
}
