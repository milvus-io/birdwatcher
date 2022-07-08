package states

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	metaPath = `meta`
)

// getConnectCommand returns the command for connect etcd.
// usage: connect --etcd [address] --rootPath [rootPath]
func getConnectCommand(state State) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "connect [options]",
		Short: "Connect to etcd instance",
		RunE: func(cmd *cobra.Command, args []string) error {
			etcdAddr, err := cmd.Flags().GetString("etcd")
			if err != nil {
				return err
			}
			rootPath, err := cmd.Flags().GetString("rootPath")
			if err != nil {
				return err
			}
			metaPath, err := cmd.Flags().GetString("metaPath")
			if err != nil {
				return err
			}

			etcdCli, err := clientv3.New(clientv3.Config{
				Endpoints:   []string{etcdAddr},
				DialTimeout: time.Second * 2,
			})
			if err != nil {
				return err
			}

			fmt.Println("Using meta path:", fmt.Sprintf("%s/%s/", rootPath, metaPath))

			etcdState := getEtcdConnectedState(etcdCli, etcdAddr)

			if rootPath != "" {
				// use rootPath as instanceName
				state.SetNext(getInstanceState(etcdCli, rootPath, etcdState))
			} else {
				// rootPath empty fall back to etcd connected state
				state.SetNext(etcdState)
			}
			return nil
		},
	}
	cmd.Flags().String("etcd", "127.0.0.1:2379", "the etcd endpoint to connect")
	cmd.Flags().String("rootPath", "by-dev", "meta root path milvus is using")
	cmd.Flags().String("metaPath", metaPath, "meta path prefix")
	return cmd
}

// findMilvusInstance iterate all possible rootPath
func findMilvusInstance(cli *clientv3.Client) ([]string, error) {
	var apps []string
	current := ""
	for {
		resp, err := cli.Get(context.Background(), current, clientv3.WithKeysOnly(), clientv3.WithLimit(1), clientv3.WithFromKey())

		if err != nil {
			return nil, err
		}
		for _, kv := range resp.Kvs {
			key := string(kv.Key)
			parts := strings.Split(key, "/")
			apps = append(apps, parts[0])
			// next key, since '0' is the next ascii char of '/'
			current = parts[0] + "0"
		}

		if !resp.More {
			break
		}
	}

	return apps, nil
}

type etcdConnectedState struct {
	cmdState
	client *clientv3.Client
	addr   string
}

// TBD for testing only
func getEtcdConnectedState(cli *clientv3.Client, addr string) State {
	cmd := &cobra.Command{
		Use:   "",
		Short: "",
	}

	state := &etcdConnectedState{
		cmdState: cmdState{
			label:   fmt.Sprintf("Etcd(%s)", addr),
			rootCmd: cmd,
		},
		client: cli,
		addr:   addr,
	}

	cmd.AddCommand()

	return state
}

func (s *etcdConnectedState) Close() {
	s.client.Close()
}
