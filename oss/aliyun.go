package oss

import (
	"fmt"
	"os"

	"github.com/aliyun/credentials-go/credentials"
	"github.com/cockroachdb/errors"
	"github.com/minio/minio-go/v7"
	minioCred "github.com/minio/minio-go/v7/pkg/credentials"
)

var newAliyunCredential = func(cfg *credentials.Config) (Credential, error) {
	return credentials.NewCredential(cfg)
}

const aliyunRoleArnEnv = "ALIBABA_CLOUD_ROLE_ARN"

func newAliyunCredentialChain(firstHopRoleARN string, targetCfg *credentials.Config) (Credential, error) {
	firstHopCfg := new(credentials.Config)
	firstHopCfg.SetType("oidc_role_arn")
	firstHopCfg.SetRoleArn(firstHopRoleARN)
	firstHopCfg.SetOIDCTokenFilePath(os.Getenv(aliyunOIDCTokenFileEnv))
	firstHopCfg.SetOIDCProviderArn(os.Getenv(aliyunOIDCProviderArnEnv))
	if targetCfg.RoleSessionExpiration != nil {
		firstHopCfg.RoleSessionExpiration = targetCfg.RoleSessionExpiration
	}
	if targetCfg.RoleSessionName != nil {
		firstHopCfg.RoleSessionName = targetCfg.RoleSessionName
	}
	if targetCfg.STSEndpoint != nil {
		firstHopCfg.STSEndpoint = targetCfg.STSEndpoint
	}

	firstHopCred, err := newAliyunCredential(firstHopCfg)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create aliyun oidc first-hop credential")
	}
	firstHopModel, err := firstHopCred.GetCredential()
	if err != nil {
		return nil, errors.Wrap(err, "failed to get aliyun oidc first-hop credential")
	}
	if firstHopModel == nil || firstHopModel.AccessKeyId == nil || firstHopModel.AccessKeySecret == nil || firstHopModel.SecurityToken == nil {
		return nil, errors.New("aliyun oidc first-hop credential is incomplete")
	}

	secondHopCfg := new(credentials.Config)
	secondHopCfg.SetType("ram_role_arn")
	if targetCfg.RoleArn != nil {
		secondHopCfg.RoleArn = targetCfg.RoleArn
	}
	if targetCfg.RoleSessionName != nil {
		secondHopCfg.RoleSessionName = targetCfg.RoleSessionName
	}
	if targetCfg.ExternalId != nil {
		secondHopCfg.ExternalId = targetCfg.ExternalId
	}
	if targetCfg.RoleSessionExpiration != nil {
		secondHopCfg.RoleSessionExpiration = targetCfg.RoleSessionExpiration
	}
	if targetCfg.STSEndpoint != nil {
		secondHopCfg.STSEndpoint = targetCfg.STSEndpoint
	}
	secondHopCfg.SetAccessKeyId(*firstHopModel.AccessKeyId)
	secondHopCfg.SetAccessKeySecret(*firstHopModel.AccessKeySecret)
	secondHopCfg.SetSecurityToken(*firstHopModel.SecurityToken)

	return newAliyunCredential(secondHopCfg)
}

func newAliyunCredentialWithFactory(cfg *credentials.Config) (Credential, error) {
	if cfg == nil {
		return newAliyunCredential(nil)
	}
	if cfg.Type == nil || *cfg.Type != "oidc_role_arn" {
		return newAliyunCredential(cfg)
	}
	envRoleARN := os.Getenv(aliyunRoleArnEnv)
	if envRoleARN == "" || cfg.RoleArn == nil || *cfg.RoleArn == envRoleARN {
		return newAliyunCredential(cfg)
	}
	return newAliyunCredentialChain(envRoleARN, cfg)
}

const (
	aliyunOIDCTokenFileEnv   = "ALIBABA_CLOUD_OIDC_TOKEN_FILE"
	aliyunOIDCProviderArnEnv = "ALIBABA_CLOUD_OIDC_PROVIDER_ARN"
)

type Credential interface {
	credentials.Credential
}

func processMinioAliyunOptions(p MinioClientParam, opts *minio.Options) error {
	switch resolveAuthMode(p) {
	case authModeRoleARN:
		credProvider, err := NewAliyunRoleCredentialProvider(p)
		if err != nil {
			return err
		}
		opts.Creds = minioCred.New(credProvider)
	case authModeIAM:
		credProvider, err := NewAliyunDefaultCredentialProvider()
		if err != nil {
			return err
		}
		opts.Creds = minioCred.New(credProvider)
	default:
		opts.Creds = minioCred.NewStaticV4(p.AK, p.SK, "")
	}
	opts.BucketLookup = minio.BucketLookupDNS
	return nil
}

func NewAliyunCredentialProvider() (minioCred.Provider, error) {
	return NewAliyunDefaultCredentialProvider()
}

func NewAliyunDefaultCredentialProvider() (minioCred.Provider, error) {
	aliyunCreds, err := newAliyunCredential(nil)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create aliyun credential")
	}
	return &CredentialProvider{aliyunCreds: aliyunCreds}, nil
}

func NewAliyunRoleCredentialProvider(p MinioClientParam) (minioCred.Provider, error) {
	cfg, err := buildAliyunRoleCredentialConfig(p)
	if err != nil {
		return nil, err
	}

	aliyunCreds, err := newAliyunCredentialWithFactory(cfg)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create aliyun role credential")
	}
	return &CredentialProvider{aliyunCreds: aliyunCreds}, nil
}

func buildAliyunRoleCredentialConfig(p MinioClientParam) (*credentials.Config, error) {
	if p.RoleARN == "" {
		return nil, errors.New("aliyun role arn is required")
	}

	cfg := new(credentials.Config)
	cfg.SetRoleArn(p.RoleARN)
	cfg.SetRoleSessionName(defaultRoleSessionName(p.RoleSessionName))
	if p.ExternalID != "" {
		cfg.ExternalId = &p.ExternalID
	}
	if p.LoadFrequency > 0 {
		cfg.SetRoleSessionExpiration(p.LoadFrequency)
	}

	switch normalizedAliyunRoleAuthMode(p.AliyunRoleAuthMode) {
	case "ram":
		cfg.SetType("ram_role_arn")
		cfg.SetAccessKeyId(p.AK)
		cfg.SetAccessKeySecret(p.SK)
	case "oidc":
		oidcTokenFile := os.Getenv(aliyunOIDCTokenFileEnv)
		oidcProviderArn := os.Getenv(aliyunOIDCProviderArnEnv)
		fmt.Printf("Aliyun OIDC credential params: %s=%q %s=%q role_arn=%q\n", aliyunOIDCTokenFileEnv, oidcTokenFile, aliyunOIDCProviderArnEnv, oidcProviderArn, p.RoleARN)
		if oidcTokenFile == "" || oidcProviderArn == "" {
			return nil, errors.New("aliyun oidc role mode requires ALIBABA_CLOUD_OIDC_TOKEN_FILE and ALIBABA_CLOUD_OIDC_PROVIDER_ARN")
		}
		cfg.SetType("oidc_role_arn")
		cfg.SetOIDCTokenFilePath(oidcTokenFile)
		cfg.SetOIDCProviderArn(oidcProviderArn)
	default:
		return nil, errors.Newf("unsupported aliyun role auth mode: %s", p.AliyunRoleAuthMode)
	}
	return cfg, nil
}

// CredentialProvider implements "github.com/minio/minio-go/v7/pkg/credentials".Provider.
type CredentialProvider struct {
	akCache     string
	aliyunCreds Credential
}

func (c *CredentialProvider) Retrieve() (minioCred.Value, error) {
	ret := minioCred.Value{}
	cred, err := c.aliyunCreds.GetCredential()
	if err != nil {
		return ret, errors.Wrap(err, "failed to get aliyun credential")
	}
	if cred == nil {
		return ret, errors.New("aliyun credential is nil")
	}
	if cred.AccessKeyId != nil {
		ret.AccessKeyID = *cred.AccessKeyId
		c.akCache = *cred.AccessKeyId
	}
	if cred.AccessKeySecret != nil {
		ret.SecretAccessKey = *cred.AccessKeySecret
	}
	if cred.SecurityToken != nil {
		ret.SessionToken = *cred.SecurityToken
	}
	return ret, nil
}

func (c CredentialProvider) IsExpired() bool {
	cred, err := c.aliyunCreds.GetCredential()
	if err != nil || cred == nil || cred.AccessKeyId == nil {
		return true
	}
	return *cred.AccessKeyId != c.akCache
}
