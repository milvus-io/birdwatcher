package states

import (
	"bufio"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

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
				prefix = basePath
			case compQueryCoord:
				prefix = fmt.Sprintf("%s/%s", basePath, `queryCoord-`)
			default:
				return fmt.Errorf("component %s not support yet", component)
			}

			now := time.Now()
			err := backupEtcd(cli, prefix, fmt.Sprintf("bw_etcd_%s.%s.bak.gz", component, now.Format("060102-150405")))
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
func backupEtcd(cli *clientv3.Client, prefix string, filePath string) error {
	resp, err := cli.Get(context.Background(), prefix, clientv3.WithCountOnly(), clientv3.WithPrefix())
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

	//TODO write backup header

	progressDisplay := uilive.New()
	progressFmt := "Backing up ... %d%%(%d/%d)\n"
	progressDisplay.Start()
	fmt.Fprintf(progressDisplay, progressFmt, 0, 0, cnt)

	currentKey := prefix
	for i := 0; int64(i) < cnt; i++ {
		resp, err = cli.Get(context.Background(), currentKey, clientv3.WithRev(rev), clientv3.WithFromKey(), clientv3.WithLimit(1))
		if err != nil {
			return err
		}

		for _, kvs := range resp.Kvs {
			w.Write(kvs.Key)
			w.WriteString("\n")
			w.Write(kvs.Value)
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
