package states

import (
	"context"
	"strconv"

	"github.com/cockroachdb/errors"
	"github.com/manifoldco/promptui"
	"github.com/minio/minio-go/v7"
	"github.com/samber/lo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/oss"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/birdwatcher/states/mgrpc"
	"github.com/milvus-io/milvus/pkg/v2/proto/rootcoordpb"
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
	_, _, _, err := s.GetMinioClientFromCfg(ctx)
	return err
}

func (s *InstanceState) GetMinioClientFromCfg(ctx context.Context, params ...oss.MinioConnectParam) (client *minio.Client, bucketName, rootPath string, err error) {
	sessions, err := common.ListSessions(ctx, s.client, s.basePath)
	if err != nil {
		return nil, "", "", err
	}

	session, found := lo.Find(sessions, func(session *models.Session) bool {
		return session.ServerName == "rootcoord" || session.ServerName == "mixcoord"
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
	source := rootcoordpb.NewRootCoordClient(conn)

	configurations, err := mgrpc.GetConfiguration(ctx, source, session.ServerID)
	if err != nil {
		return nil, "", "", err
	}

	var cloudProvider string
	var addr string
	var port string
	var ak, sk string
	var useIAM string
	var useSSL string
	var region string
	var roleARN string
	var roleSessionName string
	var externalID string
	var loadFrequency string
	var aliyunRoleAuthMode string

	for _, config := range configurations {
		switch config.GetKey() {
		case "minio.cloudprovider":
			cloudProvider = config.GetValue()
		case "minio.address":
			addr = config.GetValue()
		case "minio.port":
			port = config.GetValue()
		case "minio.region":
			region = config.GetValue()
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
		case "minio.rolearn":
			roleARN = config.GetValue()
		case "minio.rolesessionname":
			roleSessionName = config.GetValue()
		case "minio.externalid":
			externalID = config.GetValue()
		case "minio.loadfrequency":
			loadFrequency = config.GetValue()
		case "minio.aliyunroleauthmode":
			aliyunRoleAuthMode = config.GetValue()
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
		BucketName:         bucketName,
		RootPath:           rootPath,
	}
	if useIAM == "true" {
		mp.UseIAM = true
	}
	if useSSL == "true" {
		mp.UseSSL = true
	}
	if loadFrequency != "" {
		value, err := strconv.Atoi(loadFrequency)
		if err != nil {
			return nil, "", "", errors.Wrapf(err, "invalid minio.loadfrequency: %s", loadFrequency)
		}
		mp.LoadFrequency = value
	}

	for _, param := range params {
		param(&mp)
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
		Items: []string{"aws", "aliyun", "gcp"},
	}
	_, cloudProviderResult, err := cloudProvider.Run()
	if err != nil {
		return nil, "", "", err
	}

	sl := promptui.Select{
		Label: "Select authentication method:",
		Items: []string{"RoleARN", "IAM", "AK/SK"},
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
	case "RoleARN":
		input := promptui.Prompt{Label: "Role ARN"}
		roleARN, err := input.Run()
		if err != nil {
			return nil, "", "", err
		}
		mp.RoleARN = roleARN

		input = promptui.Prompt{Label: "Role Session Name (optional)"}
		roleSessionName, err := input.Run()
		if err != nil {
			return nil, "", "", err
		}
		mp.RoleSessionName = roleSessionName

		input = promptui.Prompt{Label: "External ID (optional)"}
		externalID, err := input.Run()
		if err != nil {
			return nil, "", "", err
		}
		mp.ExternalID = externalID

		input = promptui.Prompt{Label: "Load Frequency Seconds (optional)"}
		loadFrequency, err := input.Run()
		if err != nil {
			return nil, "", "", err
		}
		if loadFrequency != "" {
			value, err := strconv.Atoi(loadFrequency)
			if err != nil {
				return nil, "", "", errors.Wrapf(err, "invalid load frequency: %s", loadFrequency)
			}
			mp.LoadFrequency = value
		}

		if cloudProviderResult == oss.CloudProviderAliyun {
			mode := promptui.Select{
				Label: "Aliyun Role Auth Mode",
				Items: []string{"ram", "oidc"},
			}
			_, aliyunRoleAuthMode, err := mode.Run()
			if err != nil {
				return nil, "", "", err
			}
			mp.AliyunRoleAuthMode = aliyunRoleAuthMode
		}

		if cloudProviderResult == oss.CloudProviderAWS {
			mode := promptui.Select{
				Label: "Base Credential Source",
				Items: []string{"DefaultChain", "AK/SK"},
			}
			_, baseSource, err := mode.Run()
			if err != nil {
				return nil, "", "", err
			}
			if baseSource == "AK/SK" {
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
		}
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
