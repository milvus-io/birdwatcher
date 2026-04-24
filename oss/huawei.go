package oss

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/huaweicloud/huaweicloud-sdk-go-v3/core/auth"
	"github.com/huaweicloud/huaweicloud-sdk-go-v3/core/auth/provider"
	"github.com/huaweicloud/huaweicloud-sdk-go-v3/core/config"
	"github.com/huaweicloud/huaweicloud-sdk-go-v3/core/region"
	iam "github.com/huaweicloud/huaweicloud-sdk-go-v3/services/iam/v3"
	"github.com/huaweicloud/huaweicloud-sdk-go-v3/services/iam/v3/model"
	iamRegion "github.com/huaweicloud/huaweicloud-sdk-go-v3/services/iam/v3/region"
	"github.com/minio/minio-go/v7"
	minioCred "github.com/minio/minio-go/v7/pkg/credentials"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/log"
)

const (
	huaweiReloadCooldownNormal  = 30 * time.Second
	huaweiReloadCooldownUrgent  = 5 * time.Second
	huaweiExpirationGracePeriod = 3 * time.Minute
	huaweiTokenDurationSeconds  = int32(2 * 60 * 60)
)

func processMinioHuaweiOptions(p MinioClientParam, opts *minio.Options) error {
	if p.UseIAM {
		credProvider, err := NewHuaweiCredentialProvider()
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

// iamTokenCreator is the subset of *iam.IamClient used by HuaweiCredentialProvider.
// Defined as an interface to allow substitution in tests.
type iamTokenCreator interface {
	CreateTemporaryAccessKeyByToken(request *model.CreateTemporaryAccessKeyByTokenRequest) (*model.CreateTemporaryAccessKeyByTokenResponse, error)
}

// HuaweiCredentialProvider implements minio credentials.Provider backed by
// Huawei Cloud IAM STS (CreateTemporaryAccessKeyByToken).
type HuaweiCredentialProvider struct {
	credentials minioCred.Value
	expiration  time.Time

	basicCred auth.ICredential
	regionObj *region.Region
	iamClient iamTokenCreator

	mu     sync.Mutex
	inited bool

	refreshMu            sync.Mutex
	lastReloadFailed     bool
	lastFailedReloadTime time.Time
}

func NewHuaweiCredentialProvider() (minioCred.Provider, error) {
	return &HuaweiCredentialProvider{}, nil
}

func (p *HuaweiCredentialProvider) initClients() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.inited {
		return nil
	}

	basicChain := provider.BasicCredentialProviderChain()
	basicCred, err := basicChain.GetCredentials()
	if err != nil {
		log.Warn("HuaweiCloud credential provider: failed to get basic credentials", zap.Error(err))
		return errors.Wrap(err, "failed to get basic credentials")
	}
	p.basicCred = basicCred

	regionName := os.Getenv("HUAWEICLOUD_SDK_REGION")
	if regionName == "" {
		regionName = "cn-east-3"
	}

	regionObj, err := iamRegion.SafeValueOf(regionName)
	if err != nil {
		endpoint := fmt.Sprintf("https://iam.%s.myhuaweicloud.com", regionName)
		regionObj = region.NewRegion(regionName, endpoint)
		log.Warn("HuaweiCloud credential provider: region not in SDK, using constructed endpoint",
			zap.String("region", regionName), zap.String("endpoint", endpoint))
	}
	p.regionObj = regionObj

	hcClient, err := iam.IamClientBuilder().
		WithRegion(p.regionObj).
		WithCredential(p.basicCred).
		WithHttpConfig(config.DefaultHttpConfig().WithTimeout(30 * time.Second)).
		SafeBuild()
	if err != nil {
		log.Warn("HuaweiCloud credential provider: failed to build IAM client", zap.Error(err))
		return errors.Wrap(err, "failed to build IAM client")
	}
	p.iamClient = iam.NewIamClient(hcClient)
	p.inited = true
	log.Info("HuaweiCloud credential provider: IAM client initialized",
		zap.String("region", regionName))
	return nil
}

// isInCooldown reports whether we should skip an STS refresh due to a recent
// failure. Must be called with refreshMu held.
func (p *HuaweiCredentialProvider) isInCooldown() bool {
	if !p.lastReloadFailed {
		return false
	}
	cooldown := huaweiReloadCooldownNormal
	if p.expiration.IsZero() || time.Now().UTC().After(p.expiration) {
		cooldown = huaweiReloadCooldownUrgent
	}
	return time.Since(p.lastFailedReloadTime) < cooldown
}

// hasValidCachedCredentials reports whether the cached credentials are still
// usable. Must be called with refreshMu held.
func (p *HuaweiCredentialProvider) hasValidCachedCredentials() bool {
	return p.credentials.AccessKeyID != "" && !p.expiration.IsZero() && time.Now().UTC().Before(p.expiration)
}

func (p *HuaweiCredentialProvider) Retrieve() (minioCred.Value, error) {
	if err := p.initClients(); err != nil {
		return minioCred.Value{}, err
	}

	p.refreshMu.Lock()
	defer p.refreshMu.Unlock()

	if !p.expiration.IsZero() && time.Now().UTC().Before(p.expiration.Add(-huaweiExpirationGracePeriod)) {
		return p.credentials, nil
	}

	if p.isInCooldown() {
		if p.hasValidCachedCredentials() {
			log.Warn("HuaweiCloud credential provider: in cooldown after failure, returning cached credentials",
				zap.Time("cached_expiration", p.expiration))
			return p.credentials, nil
		}
		return minioCred.Value{}, errors.New("STS refresh in cooldown, no valid cached credentials available")
	}

	durationSeconds := huaweiTokenDurationSeconds
	request := &model.CreateTemporaryAccessKeyByTokenRequest{
		Body: &model.CreateTemporaryAccessKeyByTokenRequestBody{
			Auth: &model.TokenAuth{
				Identity: &model.TokenAuthIdentity{
					Methods: []model.TokenAuthIdentityMethods{model.GetTokenAuthIdentityMethodsEnum().TOKEN},
					Token: &model.IdentityToken{
						DurationSeconds: &durationSeconds,
					},
				},
			},
		},
	}

	response, err := p.iamClient.CreateTemporaryAccessKeyByToken(request)
	if err != nil {
		p.lastReloadFailed = true
		p.lastFailedReloadTime = time.Now()
		if p.hasValidCachedCredentials() {
			log.Warn("HuaweiCloud credential provider: STS refresh failed, falling back to cached credentials",
				zap.Time("cached_expiration", p.expiration), zap.Error(err))
			return p.credentials, nil
		}
		log.Warn("HuaweiCloud credential provider: failed to create temporary access key", zap.Error(err))
		return minioCred.Value{}, errors.Wrap(err, "failed to create temporary access key")
	}

	if response.Credential == nil ||
		response.Credential.Access == "" || response.Credential.Secret == "" || response.Credential.Securitytoken == "" {
		p.lastReloadFailed = true
		p.lastFailedReloadTime = time.Now()
		if p.hasValidCachedCredentials() {
			log.Warn("HuaweiCloud credential provider: STS returned incomplete credentials, falling back to cached",
				zap.Time("cached_expiration", p.expiration))
			return p.credentials, nil
		}
		return minioCred.Value{}, errors.New("incomplete credential returned from Huawei Cloud (missing ak/sk/token)")
	}

	expiration, err := time.Parse(time.RFC3339, response.Credential.ExpiresAt)
	if err != nil {
		p.lastReloadFailed = true
		p.lastFailedReloadTime = time.Now()
		log.Warn("HuaweiCloud credential provider: failed to parse expiration time",
			zap.String("expires_at", response.Credential.ExpiresAt), zap.Error(err))
		if p.hasValidCachedCredentials() {
			return p.credentials, nil
		}
		return minioCred.Value{}, errors.Wrap(err, "failed to parse expiration time")
	}

	credentials := minioCred.Value{
		AccessKeyID:     response.Credential.Access,
		SecretAccessKey: response.Credential.Secret,
		SessionToken:    response.Credential.Securitytoken,
		Expiration:      expiration,
		SignerType:      minioCred.SignatureV4,
	}

	p.credentials = credentials
	p.expiration = expiration
	p.lastReloadFailed = false

	akPrefix := response.Credential.Access
	if len(akPrefix) > 4 {
		akPrefix = akPrefix[:4] + "***"
	}
	log.Info("HuaweiCloud credential provider: credentials retrieved",
		zap.String("ak_prefix", akPrefix), zap.Time("expiration", expiration))

	return credentials, nil
}

// IsExpired always returns true so that minio's Credentials.Get() always calls
// Retrieve(). Retrieve() has its own cache-hit fast path using p.expiration, so
// the overhead per S3 request is a single mutex lock/unlock plus a time check.
func (p *HuaweiCredentialProvider) IsExpired() bool {
	return true
}
