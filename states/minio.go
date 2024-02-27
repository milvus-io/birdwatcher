package states

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/manifoldco/promptui"
	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/oss"
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

	var cloudProvider string
	var addr string
	var port string
	var ak, sk string
	var useIAM string
	var useSSL string

	for _, config := range configurations {
		switch config.GetKey() {
		case "minio.cloudprovider":
			cloudProvider = config.GetValue()
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

	mp := oss.MinioClientParam{
		CloudProvider: cloudProvider,
		Addr:          addr,
		Port:          port,
		AK:            ak,
		SK:            sk,

		BucketName: bucketName,
		RootPath:   rootPath,
	}
	if useIAM == "true" {
		mp.UseIAM = true
	}
	if useSSL == "true" {
		mp.UseSSL = true
	}

	mClient, err := oss.NewMinioClient(ctx, mp)
	if err != nil {
		return nil, "", "", err
	}

	return mClient.Client, bucketName, rootPath, nil
}

func (s *InstanceState) GetMinioClientFromPrompt(ctx context.Context) (client *minio.Client, bucketName, rootPath string, err error) {

	p := promptui.Prompt{
		Label: "BucketName",
	}
	bucketName, err = p.Run()
	if err != nil {
		return nil, "", "", err
	}

	p = promptui.Prompt{
		Label: "Root Path",
	}
	rootPath, err = p.Run()
	if err != nil {
		return nil, "", "", err
	}

	p = promptui.Prompt{Label: "Address"}
	address, err := p.Run()
	if err != nil {
		return nil, "", "", err
	}

	ssl := promptui.Select{
		Label: "Use SSL",
		Items: []string{"yes", "no"},
	}
	_, sslResult, err := ssl.Run()
	if err != nil {
		return nil, "", "", err
	}
	useSSL := false
	switch sslResult {
	case "yes":
		useSSL = true
	case "no":
		useSSL = false
	}

	cloudProvider := promptui.Select{
		Label: "Select Cloud provider",
		Items: []string{"aws", "gcp"},
	}
	_, cloudProviderResult, err := cloudProvider.Run()
	if err != nil {
		return nil, "", "", err
	}

	sl := promptui.Select{
		Label: "Select authentication method:",
		Items: []string{"IAM", "AK/SK"},
	}
	_, result, err := sl.Run()
	if err != nil {
		return nil, "", "", err
	}

	mp := oss.MinioClientParam{
		CloudProvider: cloudProviderResult,
		Addr:          address,
		UseSSL:        useSSL,
		BucketName:    bucketName,
		RootPath:      rootPath,
	}

	switch result {
	case "IAM":
		mp.UseIAM = true
		input := promptui.Prompt{
			Label: "IAM Endpoint",
		}
		iamEndpoint, err := input.Run()
		if err != nil {
			return nil, "", "", err
		}

		mp.IAMEndpoint = iamEndpoint
	case "AK/SK":
		p.HideEntered = true
		p.Mask = rune('*')
		p.Label = "AK"
		ak, err := p.Run()
		if err != nil {
			return nil, "", "", err
		}
		p.Label = "SK"
		sk, err := p.Run()
		if err != nil {
			return nil, "", "", err
		}

		mp.AK = ak
		mp.SK = sk
	}

	mClient, err := oss.NewMinioClient(ctx, mp)
	if err != nil {
		return nil, "", "", err
	}

	return mClient.Client, bucketName, rootPath, nil
}
