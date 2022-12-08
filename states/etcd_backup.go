package states

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/gosuri/uilive"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/proto/v2.0/commonpb"
	"github.com/milvus-io/birdwatcher/proto/v2.0/datapb"
	"github.com/milvus-io/birdwatcher/proto/v2.0/indexpb"
	"github.com/milvus-io/birdwatcher/proto/v2.0/querypb"
	"github.com/milvus-io/birdwatcher/proto/v2.0/rootcoordpb"
	datapbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/datapb"
	indexpbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/indexpb"
	internalpbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/internalpb"
	querypbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/querypb"
	rootcoordpbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/rootcoordpb"
	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
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
		Run: func(cmd *cobra.Command, args []string) {

			ignoreRevision, err := cmd.Flags().GetBool("ignoreRevision")
			if err != nil {
				fmt.Println(err.Error())
				return
			}

			prefix := ""
			switch component {
			case compAll:
				prefix = ""
			case compQueryCoord:
				prefix = `queryCoord-`
			default:
				fmt.Printf("component %s not supported for separate backup, use ALL instead\n", component)
				return
			}

			f, err := getBackupFile(component.String())
			if err != nil {
				fmt.Println("failed to open backup file:", err.Error())
				return
			}
			defer f.Close()

			gw := gzip.NewWriter(f)
			defer gw.Close()
			w := bufio.NewWriter(gw)

			// write backup header
			// version 2 used for now
			err = writeBackupHeader(w, 2)

			err = backupEtcdV2(cli, basePath, prefix, w, ignoreRevision)
			if err != nil {
				fmt.Printf("backup etcd failed, error: %v\n", err)
			}
			backupMetrics(cli, basePath, w)
			backupConfiguration(cli, basePath, w)
			backupAppMetrics(cli, basePath, w)
			fmt.Printf("backup for prefix done, stored in file: %s\n", f.Name())
		},
	}

	cmd.Flags().Var(&component, "ALL", "component to backup")
	cmd.Flags().Bool("ignoreRevision", false, "backup ignore revision change, ONLY shall works with no nodes online")
	return cmd
}

func getBackupFile(component string) (*os.File, error) {
	now := time.Now()
	filePath := fmt.Sprintf("bw_etcd_%s.%s.bak.gz", component, now.Format("060102-150405"))
	f, err := os.OpenFile(filePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}
	return f, nil
}

func writeBackupHeader(w io.Writer, version int32) error {
	lb := make([]byte, 8)
	header := &models.BackupHeader{Version: version}
	bs, err := proto.Marshal(header)
	if err != nil {
		fmt.Println("failed to marshal backup header,", err.Error())
		return err
	}
	binary.LittleEndian.PutUint64(lb, uint64(len(bs)))
	w.Write(lb)
	w.Write(bs)
	return nil
}

func backupEtcdV2(cli *clientv3.Client, base, prefix string, w *bufio.Writer, ignoreRevision bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	resp, err := cli.Get(ctx, path.Join(base, prefix), clientv3.WithCountOnly(), clientv3.WithPrefix())
	if err != nil {
		return err
	}

	if ignoreRevision {
		fmt.Println("WARNING!!! doing backup ignore revision! please make sure no instanc of milvus is online!")
	}

	// meta stored in extra
	meta := make(map[string]string)

	cnt := resp.Count
	rev := resp.Header.Revision
	meta["cnt"] = fmt.Sprintf("%d", cnt)
	meta["rev"] = fmt.Sprintf("%d", cnt)
	var instance, metaPath string
	parts := strings.Split(base, "/")
	if len(parts) > 1 {
		metaPath = parts[len(parts)-1]
		instance = path.Join(parts[:len(parts)-1]...)
	} else {
		instance = base
	}
	meta["instance"] = instance
	meta["metaPath"] = metaPath

	bs, _ := json.Marshal(meta)
	ph := models.PartHeader{
		PartType: int32(models.EtcdBackup),
		PartLen:  -1, // not sure for length
		Extra:    bs,
	}
	bs, err = proto.Marshal(&ph)
	if err != nil {
		fmt.Println("failed to marshal part header for etcd backup", err.Error())
		return err
	}
	writeBackupBytes(w, bs)

	progressDisplay := uilive.New()
	progressFmt := "Backing up ... %d%%(%d/%d)\n"
	progressDisplay.Start()
	fmt.Fprintf(progressDisplay, progressFmt, 0, 0, cnt)

	options := []clientv3.OpOption{clientv3.WithFromKey(), clientv3.WithLimit(1)}
	if !ignoreRevision {
		options = append(options, clientv3.WithRev(rev))
	}

	currentKey := path.Join(base, prefix)
	for i := 0; int64(i) < cnt; i++ {

		resp, err = cli.Get(context.Background(), currentKey, options...)
		if err != nil {
			return err
		}

		for _, kvs := range resp.Kvs {

			entry := &commonpb.KeyDataPair{Key: string(kvs.Key), Data: kvs.Value}
			bs, err = proto.Marshal(entry)
			if err != nil {
				fmt.Println("failed to marshal kv pair", err.Error())
				return err
			}

			writeBackupBytes(w, bs)

			currentKey = string(append(kvs.Key, 0))
		}

		progress := (i + 1) * 100 / int(cnt)
		fmt.Fprintf(progressDisplay, progressFmt, progress, i+1, cnt)
	}
	w.Flush()
	progressDisplay.Stop()

	// write stopper
	writeBackupBytes(w, nil)

	w.Flush()

	fmt.Printf("backup etcd for prefix %s done\n", prefix)
	return nil
}

func backupMetrics(cli *clientv3.Client, basePath string, w *bufio.Writer) error {
	sessions, err := listSessions(cli, basePath)
	if err != nil {
		return err
	}

	ph := models.PartHeader{
		PartType: int32(models.MetricsBackup),
		PartLen:  -1, // not sure for length
	}
	// write stopper
	bs, err := proto.Marshal(&ph)
	if err != nil {
		fmt.Println("failed to marshal part header for etcd backup", err.Error())
		return err
	}
	writeBackupBytes(w, bs)
	defer writeBackupBytes(w, nil)

	for _, session := range sessions {
		mbs, dmbs, err := fetchInstanceMetrics(session)
		if err != nil {
			fmt.Printf("failed to fetch metrics for %s(%d), %s\n", session.ServerName, session.ServerID, err.Error())
			continue
		}

		bs, err := json.Marshal(session)
		if err != nil {
			continue
		}
		// [session info]
		// [metrics]
		// [default metrics]
		writeBackupBytes(w, bs)
		writeBackupBytes(w, mbs)
		writeBackupBytes(w, dmbs)
	}

	return nil
}

func backupAppMetrics(cli *clientv3.Client, basePath string, w *bufio.Writer) error {
	sessions, err := listSessions(cli, basePath)
	if err != nil {
		return err
	}

	ph := models.PartHeader{
		PartType: int32(models.AppMetrics),
		PartLen:  -1, // not sure for length
	}
	// write stopper
	bs, err := proto.Marshal(&ph)
	if err != nil {
		fmt.Println("failed to marshal part header for etcd backup", err.Error())
		return err
	}
	writeBackupBytes(w, bs)
	defer writeBackupBytes(w, nil)

	for _, session := range sessions {
		opts := []grpc.DialOption{
			grpc.WithInsecure(),
			grpc.WithBlock(),
			grpc.WithTimeout(2 * time.Second),
		}

		conn, err := grpc.DialContext(context.Background(), session.Address, opts...)
		if err != nil {
			fmt.Printf("failed to connect %s(%d), err: %s\n", session.ServerName, session.ServerID, err.Error())
			continue
		}

		var client metricsSource
		switch strings.ToLower(session.ServerName) {
		case "rootcoord":
			client = rootcoordpb.NewRootCoordClient(conn)
		case "datacoord":
			client = datapb.NewDataCoordClient(conn)
		case "indexcoord":
			client = indexpb.NewIndexCoordClient(conn)
		case "querycoord":
			client = querypb.NewQueryCoordClient(conn)
		case "datanode":
			client = datapb.NewDataNodeClient(conn)
		case "querynode":
			client = querypb.NewQueryNodeClient(conn)
		case "indexnode":
			client = indexpb.NewIndexNodeClient(conn)
		}
		if client == nil {
			continue
		}
		data, err := getMetrics(context.Background(), client)
		if err != nil {
			continue
		}

		labelBs, err := json.Marshal(session)
		if err != nil {
			continue
		}

		writeBackupBytes(w, labelBs)
		writeBackupBytes(w, []byte(data))
	}

	return nil

}

func backupConfiguration(cli *clientv3.Client, basePath string, w *bufio.Writer) error {
	sessions, err := listSessions(cli, basePath)
	if err != nil {
		return err
	}

	ph := models.PartHeader{
		PartType: int32(models.Configurations),
		PartLen:  -1, // not sure for length
	}
	// write stopper
	bs, err := proto.Marshal(&ph)
	if err != nil {
		fmt.Println("failed to marshal part header for etcd backup", err.Error())
		return err
	}
	writeBackupBytes(w, bs)
	defer writeBackupBytes(w, nil)

	for _, session := range sessions {
		opts := []grpc.DialOption{
			grpc.WithInsecure(),
			grpc.WithBlock(),
			grpc.WithTimeout(2 * time.Second),
		}

		conn, err := grpc.DialContext(context.Background(), session.Address, opts...)
		if err != nil {
			fmt.Printf("failed to connect %s(%d), err: %s\n", session.ServerName, session.ServerID, err.Error())
			continue
		}

		var client configurationSource
		switch strings.ToLower(session.ServerName) {
		case "rootcoord":
			client = rootcoordpbv2.NewRootCoordClient(conn)
		case "datacoord":
			client = datapbv2.NewDataCoordClient(conn)
		case "indexcoord":
			client = indexpbv2.NewIndexCoordClient(conn)
		case "querycoord":
			client = querypbv2.NewQueryCoordClient(conn)
		case "datanode":
			client = datapbv2.NewDataNodeClient(conn)
		case "querynode":
			client = querypbv2.NewQueryNodeClient(conn)
		case "indexnode":
			client = indexpbv2.NewIndexNodeClient(conn)
		}
		if client == nil {
			continue
		}
		configurations, err := getConfiguration(context.Background(), client, session.ServerID)
		if err != nil {
			continue
		}

		labelBs, err := json.Marshal(session)
		if err != nil {
			continue
		}

		// wrap with response
		model := &internalpbv2.ShowConfigurationsResponse{
			Configuations: configurations,
		}
		bs, err := proto.Marshal(model)
		if err != nil {
			continue
		}

		writeBackupBytes(w, labelBs)
		writeBackupBytes(w, bs)
	}

	return nil
}

func writeBackupBytes(w *bufio.Writer, data []byte) {
	lb := make([]byte, 8)
	binary.LittleEndian.PutUint64(lb, uint64(len(data)))
	w.Write(lb)
	if len(data) > 0 {
		w.Write(data)
	}
}

func readBackupBytes(rd io.Reader) ([]byte, uint64, error) {
	lb := make([]byte, 8)
	var nextBytes uint64
	bsRead, err := io.ReadFull(rd, lb) //rd.Read(lb)
	// all file read
	if err == io.EOF {
		return nil, nextBytes, err
	}
	if err != nil {
		fmt.Println("failed to read file:", err.Error())
		return nil, nextBytes, err
	}
	if bsRead < 8 {
		fmt.Printf("fail to read next length %d instead of 8 read\n", bsRead)
		return nil, nextBytes, errors.New("invalid file format")
	}

	nextBytes = binary.LittleEndian.Uint64(lb)

	data := make([]byte, nextBytes)
	// cannot use rd.Read(bs), since proto marshal may generate a stopper
	bsRead, err = io.ReadFull(rd, data)
	if err != nil {
		return nil, nextBytes, err
	}
	if uint64(bsRead) != nextBytes {
		fmt.Printf("bytesRead(%d)is not equal to nextBytes(%d)\n", bsRead, nextBytes)
		return nil, nextBytes, err
	}
	return data, nextBytes, nil
}
