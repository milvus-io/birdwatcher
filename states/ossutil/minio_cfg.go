package ossutil

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/minio/minio-go/v7"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/milvus-io/birdwatcher/oss"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/birdwatcher/states/kv"
	"github.com/milvus-io/birdwatcher/states/mgrpc"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/proto/rootcoordpb"
)

func NewMinioClientParamFromConfigurations(configurations []*commonpb.KeyValuePair) (oss.MinioClientParam, error) {
	var cloudProvider, addr, port, ak, sk, useIAM, useSSL, region, bucketName, rootPath string
	var roleARN, roleSessionName, externalID, loadFrequency, aliyunRoleAuthMode string

	for _, kv := range configurations {
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
		case "minio.rolearn":
			roleARN = kv.GetValue()
		case "minio.rolesessionname":
			roleSessionName = kv.GetValue()
		case "minio.externalid":
			externalID = kv.GetValue()
		case "minio.loadfrequency":
			loadFrequency = kv.GetValue()
		case "minio.aliyunroleauthmode":
			aliyunRoleAuthMode = kv.GetValue()
		}
	}

	mp := oss.MinioClientParam{
		CloudProvider:      cloudProvider,
		Region:             region,
		Addr:               addr,
		Port:               port,
		AK:                 ak,
		SK:                 sk,
		RoleARN:            roleARN,
		RoleSessionName:    roleSessionName,
		ExternalID:         externalID,
		AliyunRoleAuthMode: aliyunRoleAuthMode,
		UseSSL:             useSSL == "true",
		BucketName:         bucketName,
		RootPath:           rootPath,
	}
	if useIAM == "true" {
		mp.UseIAM = true
	}
	if loadFrequency != "" {
		value, err := strconv.Atoi(loadFrequency)
		if err != nil {
			return oss.MinioClientParam{}, fmt.Errorf("invalid minio.loadfrequency: %s: %w", loadFrequency, err)
		}
		mp.LoadFrequency = value
	}
	return mp, nil
}

func NewResolvedObjectStore(ctx context.Context, mp oss.MinioClientParam, params ...oss.MinioConnectParam) (*oss.ResolvedObjectStore, error) {
	for _, p := range params {
		if p != nil {
			p(&mp)
		}
	}
	m, err := oss.NewMinioClient(ctx, mp)
	if err != nil {
		return nil, err
	}
	return &oss.ResolvedObjectStore{
		Store:      oss.NewMinioObjectStore(m),
		BucketName: m.BucketName,
		RootPath:   m.RootPath,
	}, nil
}

func NewResolvedObjectStoreFromConfigurations(ctx context.Context, configurations []*commonpb.KeyValuePair, params ...oss.MinioConnectParam) (*oss.ResolvedObjectStore, error) {
	mp, err := NewMinioClientParamFromConfigurations(configurations)
	if err != nil {
		return nil, err
	}
	return NewResolvedObjectStore(ctx, mp, params...)
}

func GetObjectStoreFromCfg(ctx context.Context, cli kv.MetaKV, basePath string, params ...oss.MinioConnectParam) (*oss.ResolvedObjectStore, error) {
	sessions, err := common.ListSessions(ctx, cli, basePath)
	if err != nil {
		return nil, err
	}

	for _, session := range sessions {
		opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock()}
		conn, err := grpc.DialContext(ctx, session.Address, opts...)
		if err != nil {
			continue
		}
		var source mgrpc.ConfigurationSource
		switch strings.ToLower(session.ServerName) {
		case "rootcoord", "mixcoord":
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
			conn.Close()
			continue
		}
		items, err := mgrpc.GetConfiguration(ctx, source, session.ServerID)
		conn.Close()
		if err != nil || len(items) == 0 {
			continue
		}
		mp, err := NewMinioClientParamFromConfigurations(items)
		if err != nil {
			return nil, err
		}
		if mp.BucketName == "" || mp.Addr == "" {
			continue
		}
		return NewResolvedObjectStore(ctx, mp, params...)
	}
	return nil, fmt.Errorf("minio configuration not found from any component")
}

// GetMinioClientFromCfg fetches object storage configuration from any available component and builds a minio client.
// It mirrors the behavior used by scan-binlog, while avoiding import cycles.
func GetMinioClientFromCfg(ctx context.Context, cli kv.MetaKV, basePath string, params ...oss.MinioConnectParam) (*minio.Client, string, string, error) {
	resolved, err := GetObjectStoreFromCfg(ctx, cli, basePath, params...)
	if err != nil {
		return nil, "", "", err
	}
	client, ok := oss.MinioClientFromObjectStore(resolved.Store)
	if !ok {
		return nil, "", "", fmt.Errorf("resolved object store is not backed by minio client")
	}
	return client, resolved.BucketName, resolved.RootPath, nil
}
