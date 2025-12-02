package states

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/birdwatcher/states/kv"
	"github.com/milvus-io/birdwatcher/states/mgrpc"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/proto/rootcoordpb"
)

type milvusComponent string

const (
	compAll        milvusComponent = "ALL"
	compQueryCoord milvusComponent = "QUERYCOORD"
	compRootCoord  milvusComponent = "ROOTCOORD"
	compDataCoord  milvusComponent = "DATACOORD"
	compIndexCoord milvusComponent = "INDEXCOORD"

	compQueryNode milvusComponent = "QUERYNODE"
	compDataNode  milvusComponent = "DATANODE"
	compProxy     milvusComponent = "PROXY"

	compMixCoord milvusComponent = "MIXCOORD"
)

// String implements pflag.Value.
func (c *milvusComponent) String() string {
	return string(*c)
}

// Set implements pflag.Value.
func (c *milvusComponent) Set(v string) error {
	switch strings.ToUpper(v) {
	case string(compAll), string(compQueryCoord), string(compRootCoord), string(compDataCoord), string(compIndexCoord),
		string(compQueryNode), string(compMixCoord):
		*c = milvusComponent(strings.ToUpper(v))
	default:
		return errors.New(`must be one of "ALL", "MixCoord", "QueryCoord", "DataCoord", "IndexCoord" or "RootCoord"`)
	}
	return nil
}

// Type implements pflag.Value.
func (c *milvusComponent) Type() string {
	return "MilvusComponent"
}

type BackupParam struct {
	framework.ParamBase `use:"backup" desc:"backup etcd key-values"`
	// Component string `name:""`
	component      milvusComponent
	IgnoreRevision bool  `name:"ignoreRevision" default:"false" desc:"backup ignore revision change, ONLY shall works with no nodes online"`
	BatchSize      int64 `name:"batchSize" default:"100" desc:"batch fetch size for etcd backup operation"`
}

func (p *BackupParam) ParseArgs(args []string) error {
	if len(args) == 0 {
		p.component.Set("ALL")
		return nil
	}

	return p.component.Set(args[0])
}

// getBackupEtcdCmd returns command for backup etcd
// usage: backup [component] [options...]
func (s *InstanceState) BackupCommand(ctx context.Context, p *BackupParam) error {
	prefix := ""
	switch p.component {
	case compAll:
		prefix = ""
	case compQueryCoord:
		prefix = `queryCoord-`
	default:
		return fmt.Errorf("component %s not supported for separate backup, use ALL instead", p.component.String())
	}

	f, err := getBackupFile(p.component.String())
	if err != nil {
		return errors.Wrap(err, "failed to open backup file")
	}
	defer f.Close()

	gw := gzip.NewWriter(f)
	defer gw.Close()
	w := bufio.NewWriter(gw)

	// write backup header
	// version 2 used for now
	err = writeBackupHeader(w, 2)
	if err != nil {
		return errors.Wrap(err, "failed to write backup file header")
	}

	err = backupEtcdV2(s.client, s.basePath, prefix, w, p)
	if err != nil {
		fmt.Printf("backup etcd failed, error: %v\n", err)
	}
	backupMetrics(s.client, s.basePath, w)
	backupConfiguration(s.client, s.basePath, w)
	backupAppMetrics(s.client, s.basePath, w)
	fmt.Printf("backup for prefix done, stored in file: %s\n", f.Name())
	return nil
}

func getBackupFile(component string) (*os.File, error) {
	now := time.Now()
	filePath := fmt.Sprintf("bw_etcd_%s.%s.bak.gz", component, now.Format("060102-150405"))
	f, err := os.OpenFile(filePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
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

func backupEtcdV2(cli kv.MetaKV, base, prefix string, w *bufio.Writer, opt *BackupParam) error {
	return cli.BackupKV(base, prefix, w, opt.IgnoreRevision, opt.BatchSize)
}

func backupMetrics(cli kv.MetaKV, basePath string, w *bufio.Writer) error {
	sessions, err := common.ListSessions(context.Background(), cli, basePath)
	if err != nil {
		return err
	}

	ph := models.PartHeader{
		PartType: models.PartType_MetricsBackup,
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

func backupAppMetrics(cli kv.MetaKV, basePath string, w *bufio.Writer) error {
	sessions, err := common.ListSessions(context.Background(), cli, basePath)
	if err != nil {
		return err
	}

	ph := models.PartHeader{
		PartType: models.PartType_AppMetrics,
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
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithBlock(),
		}

		var conn *grpc.ClientConn
		var err error
		func() {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			conn, err = grpc.DialContext(ctx, session.Address, opts...)
		}()
		if err != nil {
			fmt.Printf("failed to connect %s(%d), err: %s\n", session.ServerName, session.ServerID, err.Error())
			continue
		}

		var client mgrpc.MetricsSource
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
			// case "indexnode":
			// 	client = indexpb.NewIndexNodeClient(conn)
		}
		if client == nil {
			continue
		}
		data, err := mgrpc.GetMetrics(context.Background(), client)
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

func backupConfiguration(cli kv.MetaKV, basePath string, w *bufio.Writer) error {
	sessions, err := common.ListSessions(context.Background(), cli, basePath)
	if err != nil {
		return err
	}

	ph := models.PartHeader{
		PartType: models.PartType_Configurations,
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
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithBlock(),
			grpc.WithTimeout(2 * time.Second),
		}

		conn, err := grpc.DialContext(context.Background(), session.Address, opts...)
		if err != nil {
			fmt.Printf("failed to connect %s(%d), err: %s\n", session.ServerName, session.ServerID, err.Error())
			continue
		}

		var client mgrpc.ConfigurationSource
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
			// case "indexnode":
			// 	client = indexpbv2.NewIndexNodeClient(conn)
		}
		if client == nil {
			continue
		}
		configurations, err := mgrpc.GetConfiguration(context.Background(), client, session.ServerID)
		if err != nil {
			continue
		}

		labelBs, err := json.Marshal(session)
		if err != nil {
			continue
		}

		// wrap with response
		model := &internalpb.ShowConfigurationsResponse{
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
	bsRead, err := io.ReadFull(rd, lb) // rd.Read(lb)
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
