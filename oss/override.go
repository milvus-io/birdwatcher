package oss

import (
	"context"
	"fmt"
	"strconv"
)

// OSSOverrideParam uses strings for optional booleans so omitted flags preserve
// discovered configuration, while an explicit false overrides a true value.
// It is embedded in command params to let users manually specify internal OSS
// settings when the component configuration fetched via grpc is masked or
// unavailable. The existing minioAddr/skipBucketCheck flags remain on the
// owning command.
type OSSOverrideParam struct {
	LoadFrequency      string `name:"loadFrequency" default:"" desc:"override internal OSS credential reload interval (seconds)"`
	OSSStandalone      bool   `name:"ossStandalone" default:"false" desc:"build internal OSS client from flags without fetching component configuration"`
	Port               string `name:"port" default:"" desc:"override internal OSS port"`
	AccessKeyID        string `name:"accessKeyID" default:"" desc:"override internal OSS access key ID"`
	SecretAccessKey    string `name:"secretAccessKey" default:"" desc:"override internal OSS secret access key"`
	UseIAM             string `name:"useIAM" default:"" desc:"override internal OSS IAM: true or false (omitted inherits config)"`
	UseSSL             string `name:"useSSL" default:"" desc:"override internal OSS TLS: true or false (omitted inherits config)"`
	CloudProvider      string `name:"cloudProvider" default:"" desc:"override internal OSS provider: aws, gcp, aliyun, tencent, huawei"`
	Region             string `name:"region" default:"" desc:"override internal OSS region"`
	BucketName         string `name:"bucketName" default:"" desc:"override internal OSS bucket name"`
	RootPath           string `name:"rootPath" default:"" desc:"override internal OSS root path"`
	IAMEndpoint        string `name:"iamEndpoint" default:"" desc:"override internal OSS IAM endpoint"`
	RoleARN            string `name:"roleARN" default:"" desc:"override internal OSS role ARN"`
	RoleSessionName    string `name:"roleSessionName" default:"" desc:"override internal OSS role session name"`
	ExternalID         string `name:"externalID" default:"" desc:"override internal OSS role external ID"`
	AliyunRoleAuthMode string `name:"aliyunRoleAuthMode" default:"" desc:"override internal OSS Aliyun role auth mode: ram or oidc"`
}

// ConnectParams converts the override fields into MinioConnectParam options to
// be applied on top of discovered (or empty) MinioClientParam.
func (p OSSOverrideParam) ConnectParams() ([]MinioConnectParam, error) {
	var params []MinioConnectParam
	if p.LoadFrequency != "" {
		value, err := strconv.Atoi(p.LoadFrequency)
		if err != nil || value < 0 {
			return nil, fmt.Errorf("--loadFrequency must be a non-negative integer")
		}
		params = append(params, WithLoadFrequency(value))
	}
	for _, option := range []struct {
		name, value string
		apply       func(bool) MinioConnectParam
	}{
		{"useIAM", p.UseIAM, WithUseIAM}, {"useSSL", p.UseSSL, WithUseSSL},
	} {
		if option.value == "" {
			continue
		}
		value, err := strconv.ParseBool(option.value)
		if err != nil {
			return nil, fmt.Errorf("--%s must be true or false", option.name)
		}
		params = append(params, option.apply(value))
	}
	for _, option := range []struct {
		value string
		apply func(string) MinioConnectParam
	}{
		{p.Port, WithPort}, {p.CloudProvider, WithCloudProvider}, {p.Region, WithRegion},
		{p.BucketName, WithBucketName}, {p.RootPath, WithRootPath}, {p.IAMEndpoint, WithIAMEndpoint},
		{p.RoleARN, WithRoleARN}, {p.RoleSessionName, WithRoleSessionName}, {p.ExternalID, WithExternalID},
		{p.AliyunRoleAuthMode, WithAliyunRoleAuthMode},
	} {
		if option.value != "" {
			params = append(params, option.apply(option.value))
		}
	}
	if p.AccessKeyID != "" || p.SecretAccessKey != "" {
		// Override each independently so a single supplied credential does not clear
		// the other value from the discovered configuration.
		params = append(params, func(mp *MinioClientParam) {
			if p.AccessKeyID != "" {
				mp.AK = p.AccessKeyID
			}
			if p.SecretAccessKey != "" {
				mp.SK = p.SecretAccessKey
			}
		})
	}
	return params, nil
}

// BuildOverrideConnectParams converts an OSSOverrideParam plus the owning
// command's minioAddr/skipBucketCheck flags into MinioConnectParam options.
func BuildOverrideConnectParams(p OSSOverrideParam, address string, skipBucketCheck bool) ([]MinioConnectParam, error) {
	params, err := p.ConnectParams()
	if err != nil {
		return nil, err
	}
	params = append(params, WithSkipCheckBucket(skipBucketCheck))
	if address != "" {
		params = append(params, WithMinioAddr(address))
	}
	return params, nil
}

// NewResolvedObjectStoreStandalone builds a resolved object store purely from
// the supplied override flags, without fetching component configuration.
func NewResolvedObjectStoreStandalone(ctx context.Context, p OSSOverrideParam, address string, skipBucketCheck bool) (*ResolvedObjectStore, error) {
	if address == "" || p.BucketName == "" || p.CloudProvider == "" {
		return nil, fmt.Errorf("--ossStandalone requires --minioAddr, --bucketName and --cloudProvider")
	}
	params, err := BuildOverrideConnectParams(p, address, skipBucketCheck)
	if err != nil {
		return nil, err
	}
	return NewResolvedObjectStore(ctx, MinioClientParam{}, params...)
}

// GetObjectStoreFunc builds a resolved object store from discovered
// configuration, applying the given connect params on top of it.
type GetObjectStoreFunc func(ctx context.Context, params ...MinioConnectParam) (*ResolvedObjectStore, error)

// NewResolvedObjectStoreWithOverrides builds a resolved object store from an
// OSSOverrideParam. Unless --ossStandalone is set, the base configuration is
// discovered through getStore; standalone mode builds the client purely from
// the supplied flags. getStore may be nil when standalone is set.
func NewResolvedObjectStoreWithOverrides(ctx context.Context, p OSSOverrideParam, address string, skipBucketCheck bool, getStore GetObjectStoreFunc) (*ResolvedObjectStore, error) {
	if !p.OSSStandalone {
		if getStore == nil {
			return nil, fmt.Errorf("object store provider is nil")
		}
		params, err := BuildOverrideConnectParams(p, address, skipBucketCheck)
		if err != nil {
			return nil, err
		}
		return getStore(ctx, params...)
	}
	return NewResolvedObjectStoreStandalone(ctx, p, address, skipBucketCheck)
}
