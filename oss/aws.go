package oss

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	awsCreds "github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/cockroachdb/errors"
	minioCred "github.com/minio/minio-go/v7/pkg/credentials"
)

type awsRoleProviderMeta struct {
	baseCredentialSource string
	roleSessionName      string
}

type AWSRoleCredentialProvider struct {
	cache aws.CredentialsProvider
	meta  awsRoleProviderMeta

	mu        sync.Mutex
	lastValue minioCred.Value
	hasLast   bool
	expiresAt time.Time
}

func buildAWSRoleProviderForTest(p MinioClientParam) (minioCred.Provider, awsRoleProviderMeta, error) {
	provider, meta, err := newAWSRoleCredentialProvider(p)
	return provider, meta, err
}

func NewAWSRoleCredentialProvider(p MinioClientParam) (minioCred.Provider, error) {
	provider, _, err := newAWSRoleCredentialProvider(p)
	return provider, err
}

func newAWSRoleCredentialProvider(p MinioClientParam) (minioCred.Provider, awsRoleProviderMeta, error) {
	meta := awsRoleProviderMeta{roleSessionName: defaultRoleSessionName(p.RoleSessionName)}
	if p.RoleARN == "" {
		return nil, meta, errors.New("aws role arn is required")
	}

	cfg, err := loadAWSRoleConfig(context.Background(), p, &meta)
	if err != nil {
		return nil, meta, err
	}

	stsClient := sts.NewFromConfig(cfg)
	roleProvider := stscreds.NewAssumeRoleProvider(stsClient, p.RoleARN, func(o *stscreds.AssumeRoleOptions) {
		o.RoleSessionName = meta.roleSessionName
		if p.ExternalID != "" {
			o.ExternalID = aws.String(p.ExternalID)
		}
		if p.LoadFrequency > 0 {
			o.Duration = time.Duration(p.LoadFrequency) * time.Second
		}
	})

	return &AWSRoleCredentialProvider{
		cache: aws.NewCredentialsCache(roleProvider),
		meta:  meta,
	}, meta, nil
}

func loadAWSRoleConfig(ctx context.Context, p MinioClientParam, meta *awsRoleProviderMeta) (aws.Config, error) {
	loadOptions := []func(*awsconfig.LoadOptions) error{}
	if p.Region != "" {
		loadOptions = append(loadOptions, awsconfig.WithRegion(p.Region))
	}

	if p.AK != "" && p.SK != "" {
		meta.baseCredentialSource = "static"
		provider := awsCreds.NewStaticCredentialsProvider(p.AK, p.SK, "")
		loadOptions = append(loadOptions, awsconfig.WithCredentialsProvider(provider))
	} else {
		meta.baseCredentialSource = "default-chain"
	}

	cfg, err := awsconfig.LoadDefaultConfig(ctx, loadOptions...)
	if err != nil {
		return aws.Config{}, errors.Wrap(err, "failed to load aws config")
	}
	if cfg.Region == "" {
		cfg.Region = p.Region
	}
	return cfg, nil
}

func (p *AWSRoleCredentialProvider) Retrieve() (minioCred.Value, error) {
	creds, err := p.cache.Retrieve(context.Background())
	if err != nil {
		return minioCred.Value{}, errors.Wrap(err, "failed to retrieve aws assume-role credentials")
	}
	value := minioCred.Value{
		AccessKeyID:     creds.AccessKeyID,
		SecretAccessKey: creds.SecretAccessKey,
		SessionToken:    creds.SessionToken,
		Expiration:      creds.Expires,
		SignerType:      minioCred.SignatureV4,
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	p.lastValue = value
	p.hasLast = true
	p.expiresAt = creds.Expires
	return value, nil
}

func (p *AWSRoleCredentialProvider) IsExpired() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.hasLast {
		return true
	}
	if p.expiresAt.IsZero() {
		return false
	}
	return time.Now().After(p.expiresAt.Add(-time.Minute))
}

func (p *AWSRoleCredentialProvider) String() string {
	return fmt.Sprintf("AWSRoleCredentialProvider(%s)", p.meta.baseCredentialSource)
}
