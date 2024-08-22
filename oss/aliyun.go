package oss

import (
	"github.com/aliyun/credentials-go/credentials" // >= v1.2.6
	"github.com/cockroachdb/errors"
	"github.com/minio/minio-go/v7"
	minioCred "github.com/minio/minio-go/v7/pkg/credentials"
)

type Credential interface {
	credentials.Credential
}

func processMinioAliyunOptions(p MinioClientParam, opts *minio.Options) error {
	if p.UseIAM {
		credProvider, err := NewAliyunCredentialProvider()
		if err != nil {
			return err
		}
		opts.Creds = minioCred.New(credProvider)
	} else {
		opts.Creds = minioCred.NewStaticV4(p.AK, p.SK, "")
	}
	opts.BucketLookup = minio.BucketLookupDNS
	return nil
}

// CredentialProvider implements "github.com/minio/minio-go/v7/pkg/credentials".Provider
// also implements transport
type CredentialProvider struct {
	// aliyunCreds doesn't provide a way to get the expire time, so we use the cache to check if it's expired
	// when aliyunCreds.GetAccessKeyId is different from the cache, we know it's expired
	akCache     string
	aliyunCreds Credential
}

func NewAliyunCredentialProvider() (minioCred.Provider, error) {
	aliyunCreds, err := credentials.NewCredential(nil)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create aliyun credential")
	}
	// backend, err := minio.DefaultTransport(true)
	// if err != nil {
	// 	return nil, errors.Wrap(err, "failed to create default transport")
	// }
	// credentials.GetCredential()
	return &CredentialProvider{aliyunCreds: aliyunCreds}, nil
}

// Retrieve returns nil if it successfully retrieved the value.
// Error is returned if the value were not obtainable, or empty.
// according to the caller minioCred.Credentials.Get(),
// it already has a lock, so we don't need to worry about concurrency
func (c *CredentialProvider) Retrieve() (minioCred.Value, error) {
	ret := minioCred.Value{}
	ak, err := c.aliyunCreds.GetAccessKeyId()
	if err != nil {
		return ret, errors.Wrap(err, "failed to get access key id from aliyun credential")
	}
	ret.AccessKeyID = *ak
	sk, err := c.aliyunCreds.GetAccessKeySecret()
	if err != nil {
		return minioCred.Value{}, errors.Wrap(err, "failed to get access key secret from aliyun credential")
	}
	securityToken, err := c.aliyunCreds.GetSecurityToken()
	if err != nil {
		return minioCred.Value{}, errors.Wrap(err, "failed to get security token from aliyun credential")
	}
	ret.SecretAccessKey = *sk
	c.akCache = *ak
	ret.SessionToken = *securityToken
	return ret, nil
}

// IsExpired returns if the credentials are no longer valid, and need
// to be retrieved.
// according to the caller minioCred.Credentials.IsExpired(),
// it already has a lock, so we don't need to worry about concurrency
func (c CredentialProvider) IsExpired() bool {
	ak, err := c.aliyunCreds.GetAccessKeyId()
	if err != nil {
		return true
	}
	return *ak != c.akCache
}
