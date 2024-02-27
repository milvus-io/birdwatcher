package states

import (
	"context"
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	rootcoordpbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/rootcoordpb"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/minio/minio-go/v7"
	"github.com/samber/lo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type TestMinioCfgParam struct {
	framework.ParamBase `use:"test-minio-cfg"`
	// *MinioConnectParam
	MinioAddress  string `name:"minioAddress"`
	MinioPasswd   string `name:"minioPassword"`
	MinioUserName string `name:"minioUsername"`
}

type MinioConnectParam struct {
	MinioAddress  string `name:"minioAddress"`
	MinioPasswd   string `name:"minioPassword"`
	MinioUserName string `name:"minioUsername"`
}

func (s *InstanceState) TestMinioCfgCommand(ctx context.Context, p *TestMinioCfgParam) error {
	fmt.Println(p)
	_, _, _, err := s.GetMinioClientFromCfg(ctx, "")
	return err
}

func (s *InstanceState) GetMinioClientFromCfg(ctx context.Context, minioAddr string) (client *minio.Client, bucketName, rootPath string, err error) {
	sessions, err := common.ListSessions(s.client, s.basePath)
	if err != nil {
		return nil, "", "", err
	}

	session, found := lo.Find(sessions, func(session *models.Session) bool {
		return session.ServerName == "rootcoord"
	})

	if !found {
		return nil, "", "", errors.New("rootcoord session not found")
	}

	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	}

	conn, err := grpc.DialContext(ctx, session.Address, opts...)
	if err != nil {
		return nil, "", "", errors.New("find to connect to rootcoord")
	}
	source := rootcoordpbv2.NewRootCoordClient(conn)

	configurations, err := getConfiguration(ctx, source, session.ServerID)
	if err != nil {
		return nil, "", "", err
	}

	var addr string
	var port string
	var ak, sk string
	var useIAM string
	var useSSL string

	var secure bool

	for _, config := range configurations {
		switch config.GetKey() {
		case "minio.address":
			addr = config.GetValue()
		case "minio.port":
			port = config.GetValue()
		case "minio.bucketname":
			bucketName = config.GetValue()
		case "minio.rootpath":
			rootPath = config.GetValue()
		case "minio.secretaccesskey":
			sk = config.GetValue()
		case "minio.accesskeyid":
			ak = config.GetValue()
		case "minio.useiam":
			useIAM = config.GetValue()
		case "minio.usessl":
			useSSL = config.GetValue()
		}
	}

	if useSSL == "true" {
		secure = true
	}

	if minioAddr == "" {
		minioAddr = fmt.Sprintf("%s:%s", addr, port)
	}

	if useIAM == "true" {
		client, _, err = getMinioWithIAM(minioAddr, bucketName, secure)
	} else {
		client, _, err = getMinioWithInfo(minioAddr, ak, sk, bucketName, secure)
	}

	if err != nil {
		return nil, "", "", err
	}

	return client, bucketName, rootPath, nil
}
