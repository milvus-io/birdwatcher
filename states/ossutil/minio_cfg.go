package ossutil

import (
	"context"
	"fmt"
	"strings"

	"github.com/minio/minio-go/v7"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/milvus-io/birdwatcher/oss"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/birdwatcher/states/kv"
	"github.com/milvus-io/birdwatcher/states/mgrpc"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/proto/rootcoordpb"
)

// GetMinioClientFromCfg fetches object storage configuration from any available component and builds a minio client.
// It mirrors the behavior used by scan-binlog, while avoiding import cycles.
func GetMinioClientFromCfg(ctx context.Context, cli kv.MetaKV, basePath string, params ...oss.MinioConnectParam) (*minio.Client, string, string, error) {
	sessions, err := common.ListSessions(ctx, cli, basePath)
	if err != nil {
		return nil, "", "", err
	}

	var cloudProvider, addr, port, ak, sk, useIAM, useSSL, region, bucketName, rootPath string
	found := false
	for _, session := range sessions {
		opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock()}
		conn, err := grpc.DialContext(ctx, session.Address, opts...)
		if err != nil {
			continue
		}
		var source mgrpc.ConfigurationSource
		switch strings.ToLower(session.ServerName) {
		case "rootcoord":
			source = rootcoordpb.NewRootCoordClient(conn)
		case "datacoord":
			source = datapb.NewDataCoordClient(conn)
		case "indexcoord":
			source = indexpb.NewIndexCoordClient(conn)
		case "querycoord":
			source = querypb.NewQueryCoordClient(conn)
		case "datanode":
			source = datapb.NewDataNodeClient(conn)
		case "querynode":
			source = querypb.NewQueryNodeClient(conn)
		default:
			continue
		}
		items, err := mgrpc.GetConfiguration(ctx, source, session.ServerID)
		if err != nil || len(items) == 0 {
			continue
		}
		for _, kv := range items {
			switch kv.GetKey() {
			case "minio.cloudprovider":
				cloudProvider = kv.GetValue()
			case "minio.address":
				addr = kv.GetValue()
			case "minio.port":
				port = kv.GetValue()
			case "minio.region":
				region = kv.GetValue()
			case "minio.bucketname":
				bucketName = kv.GetValue()
			case "minio.rootpath":
				rootPath = kv.GetValue()
			case "minio.secretaccesskey":
				sk = kv.GetValue()
			case "minio.accesskeyid":
				ak = kv.GetValue()
			case "minio.useiam":
				useIAM = kv.GetValue()
			case "minio.usessl":
				useSSL = kv.GetValue()
			}
		}
		if bucketName != "" && addr != "" {
			found = true
			break
		}
	}
	if !found {
		return nil, "", "", fmt.Errorf("minio configuration not found from any component")
	}

	mp := oss.MinioClientParam{
		CloudProvider: cloudProvider,
		Region:        region,
		Addr:          addr,
		Port:          port,
		AK:            ak,
		SK:            sk,
		UseSSL:        useSSL == "true",
		BucketName:    bucketName,
		RootPath:      rootPath,
	}
	if useIAM == "true" {
		mp.UseIAM = true
	}
	for _, p := range params {
		p(&mp)
	}
	m, err := oss.NewMinioClient(ctx, mp)
	if err != nil {
		return nil, "", "", err
	}
	return m.Client, bucketName, rootPath, nil
}
