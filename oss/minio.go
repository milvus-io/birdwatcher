package oss

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

const (
	CloudProviderGCP     = "gcp"
	CloudProviderAWS     = "aws"
	CloudProviderAliyun  = "aliyun"
	CloudProviderAzure   = "azure"
	CloudProviderTencent = "tencent"
	CloudProviderHuawei  = "huawei"
)

type MinioClientParam struct {
	Addr          string
	Port          string
	AK            string
	SK            string
	UseIAM        bool
	IAMEndpoint   string
	UseSSL        bool
	CloudProvider string
	Region        string

	RoleARN            string
	RoleSessionName    string
	ExternalID         string
	LoadFrequency      int
	AliyunRoleAuthMode string

	BucketName string
	RootPath   string

	skipCheckBucket bool
}

type authMode string

const (
	authModeRoleARN authMode = "rolearn"
	authModeIAM     authMode = "iam"
	authModeStatic  authMode = "static"
)

func resolveAuthMode(p MinioClientParam) authMode {
	switch {
	case p.RoleARN != "":
		return authModeRoleARN
	case p.UseIAM:
		return authModeIAM
	default:
		return authModeStatic
	}
}

func defaultRoleSessionName(value string) string {
	if value != "" {
		return value
	}
	return "birdwatcher"
}

func normalizedAliyunRoleAuthMode(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "ram":
		return "ram"
	case "oidc":
		return "oidc"
	default:
		return strings.ToLower(strings.TrimSpace(value))
	}
}

// MinioConnectParam is the function type to override client params
type MinioConnectParam func(p *MinioClientParam)

func WithSkipCheckBucket(v bool) MinioConnectParam {
	return func(p *MinioClientParam) {
		p.skipCheckBucket = v
	}
}

func WithMinioAddr(addr string) MinioConnectParam {
	return func(p *MinioClientParam) {
		p.Addr = addr
	}
}

func WithRoleARN(v string) MinioConnectParam {
	return func(p *MinioClientParam) {
		p.RoleARN = v
	}
}

func WithRoleSessionName(v string) MinioConnectParam {
	return func(p *MinioClientParam) {
		p.RoleSessionName = v
	}
}

func WithExternalID(v string) MinioConnectParam {
	return func(p *MinioClientParam) {
		p.ExternalID = v
	}
}

func WithLoadFrequency(v int) MinioConnectParam {
	return func(p *MinioClientParam) {
		p.LoadFrequency = v
	}
}

func WithAliyunRoleAuthMode(v string) MinioConnectParam {
	return func(p *MinioClientParam) {
		p.AliyunRoleAuthMode = v
	}
}

func WithUseIAM(v bool) MinioConnectParam {
	return func(p *MinioClientParam) {
		p.UseIAM = v
	}
}

func WithUseSSL(v bool) MinioConnectParam {
	return func(p *MinioClientParam) {
		p.UseSSL = v
	}
}

func WithCloudProvider(v string) MinioConnectParam {
	return func(p *MinioClientParam) {
		p.CloudProvider = v
	}
}

func WithRegion(v string) MinioConnectParam {
	return func(p *MinioClientParam) {
		p.Region = v
	}
}

func WithBucketName(v string) MinioConnectParam {
	return func(p *MinioClientParam) {
		p.BucketName = v
	}
}

func WithRootPath(v string) MinioConnectParam {
	return func(p *MinioClientParam) {
		p.RootPath = v
	}
}

func WithPort(v string) MinioConnectParam {
	return func(p *MinioClientParam) {
		p.Port = v
	}
}

func WithAKSK(ak, sk string) MinioConnectParam {
	return func(p *MinioClientParam) {
		p.AK = ak
		p.SK = sk
	}
}

func WithIAMEndpoint(v string) MinioConnectParam {
	return func(p *MinioClientParam) {
		p.IAMEndpoint = v
	}
}

// MinioClient wraps minio client, bucket info within
type MinioClient struct {
	Client     *minio.Client
	BucketName string
	RootPath   string
}

func NewMinioClient(ctx context.Context, p MinioClientParam) (*MinioClient, error) {
	opts := &minio.Options{
		Secure:       p.UseSSL,
		BucketLookup: minio.BucketLookupAuto,
	}
	// Build endpoint safely. If Addr already includes a port (contains ':'),
	// or Port is empty, use Addr as-is; otherwise append ":Port".
	endpoint := p.Addr
	if p.Port != "" && !strings.Contains(p.Addr, ":") {
		endpoint = fmt.Sprintf("%s:%s", p.Addr, p.Port)
	}

	var err error
	switch p.CloudProvider {
	case CloudProviderAWS:
		err = processMinioAwsOptions(p, opts)
	case CloudProviderGCP:
		// adhoc to remove port of gcs address to let minio-go know it's gcs
		if strings.Contains(endpoint, GcsDefaultAddress) {
			endpoint = GcsDefaultAddress
		}
		err = processMinioGcpOptions(p, opts)
	case CloudProviderAliyun:
		err = processMinioAliyunOptions(p, opts)
	case CloudProviderTencent:
		err = processMinioTencentOptions(p, opts)
	case CloudProviderHuawei:
		err = processMinioHuaweiOptions(p, opts)
	case CloudProviderAzure:
		// TODO support azure
		fallthrough
	default:
		return nil, errors.Newf("Cloud provider %s not supported yet", p.CloudProvider)
	}
	if err != nil {
		return nil, err
	}

	fmt.Printf("Start to connect to oss endpoint: %s\n", endpoint)
	client, err := minio.New(endpoint, opts)
	if err != nil {
		fmt.Println("new client failed: ", err.Error())
		return nil, err
	}

	fmt.Println("Connection successful!")

	if p.skipCheckBucket {
		fmt.Println("Skip bucket existence check...")
	} else {
		ok, err := client.BucketExists(ctx, p.BucketName)
		if err != nil {
			fmt.Printf("check bucket %s exists failed: %s\n", p.BucketName, err.Error())
			return nil, err
		}
		if !ok {
			return nil, errors.Newf("Bucket %s not exists", p.BucketName)
		}
	}

	return &MinioClient{
		Client:     client,
		BucketName: p.BucketName,
		RootPath:   p.RootPath,
	}, nil
}

func processMinioAwsOptions(p MinioClientParam, opts *minio.Options) error {
	switch resolveAuthMode(p) {
	case authModeRoleARN:
		credProvider, err := NewAWSRoleCredentialProvider(p)
		if err != nil {
			return err
		}
		opts.Creds = credentials.New(credProvider)
	case authModeIAM:
		opts.Creds = credentials.NewIAM(p.IAMEndpoint)
	default:
		opts.Creds = credentials.NewStaticV4(p.AK, p.SK, "")
	}
	return nil
}

func processMinioStaticOptions(p MinioClientParam, opts *minio.Options) {
	opts.Creds = credentials.NewStaticV4(p.AK, p.SK, "")
}

func processMinioIAMOptions(p MinioClientParam, opts *minio.Options) {
	opts.Creds = credentials.NewIAM(p.IAMEndpoint)
}
