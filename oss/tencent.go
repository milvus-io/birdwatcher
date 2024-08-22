package oss

import (
	"github.com/cockroachdb/errors"
	"github.com/minio/minio-go/v7"
	minioCred "github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common"
)

func processMinioTencentOptions(p MinioClientParam, opts *minio.Options) error {
	if p.UseIAM {
		credProvider, err := NewTencentCredentialProvider()
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

// TencentCredentialProvider implements "github.com/minio/minio-go/v7/pkg/credentials".Provider
// also implements transport
type TencentCredentialProvider struct {
	// tencentCreds doesn't provide a way to get the expired time, so we use the cache to check if it's expired
	// when tencentCreds.GetSecretId is different from the cache, we know it's expired
	akCache      string
	tencentCreds common.CredentialIface
}

func NewTencentCredentialProvider() (minioCred.Provider, error) {
	var provider interface {
		GetCredential() (common.CredentialIface, error)
	}
	var err error
	provider, err = common.DefaultTkeOIDCRoleArnProvider()
	if err != nil {
		// try cvm role provider
		provider = common.DefaultCvmRoleProvider()

	}
	cred, err := provider.GetCredential()
	if err != nil {
		return nil, errors.Wrap(err, "failed to get tencent credential")
	}
	return &TencentCredentialProvider{tencentCreds: cred}, nil
}

// Retrieve returns nil if it successfully retrieved the value.
// Error is returned if the value were not obtainable, or empty.
// according to the caller minioCred.Credentials.Get(),
// it already has a lock, so we don't need to worry about concurrency
func (c *TencentCredentialProvider) Retrieve() (minioCred.Value, error) {
	ret := minioCred.Value{}
	ak := c.tencentCreds.GetSecretId()
	ret.AccessKeyID = ak
	c.akCache = ak

	sk := c.tencentCreds.GetSecretKey()
	ret.SecretAccessKey = sk

	securityToken := c.tencentCreds.GetToken()
	ret.SessionToken = securityToken
	return ret, nil
}

// IsExpired returns if the credentials are no longer valid, and need
// to be retrieved.
// according to the caller minioCred.Credentials.IsExpired(),
// it already has a lock, so we don't need to worry about concurrency
func (c TencentCredentialProvider) IsExpired() bool {
	ak := c.tencentCreds.GetSecretId()
	return ak != c.akCache
}
