package oss

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
)

const (
	defaultAzureSASDuration       = 900
	defaultAzureSASRequestTimeout = 3 * time.Second
	azureSASRefreshOffset         = time.Minute
	maxAzureSASBrokerResponseSize = 1 << 20
)

type azureSASBrokerConfig struct {
	endpoint        string
	region          string
	bucket          string
	clientID        string
	tenantID        string
	accountName     string
	durationSeconds int
}

type azureSASBrokerPolicy struct {
	broker *azureSASBroker
}

type azureRedactingTransport struct {
	client *http.Client
}

type azureSASBroker struct {
	config     azureSASBrokerConfig
	httpClient *http.Client
	now        func() time.Time

	mu        sync.Mutex
	token     string
	expiresAt time.Time
}

type azureSASBrokerRequest struct {
	CSP              string `json:"csp"`
	Region           string `json:"region"`
	Bucket           string `json:"bucket"`
	DurationSeconds  int    `json:"durationSeconds"`
	AzureClientID    string `json:"azureClientId"`
	AzureTenantID    string `json:"azureTenantId"`
	AzureAccountName string `json:"azureAccountName"`
}

type azureSASBrokerResponse struct {
	Success     bool                      `json:"success"`
	Credentials azureSASBrokerCredentials `json:"credentials"`
}

type azureSASBrokerCredentials struct {
	AccountName  string `json:"tempAk"`
	SessionToken string `json:"sessionToken"`
	ExpiredAt    string `json:"expiredAt"`
}

func hasAzureSASBrokerConfig(p MinioClientParam) bool {
	return p.AzureClientID != "" || p.AzureTenantID != "" || p.AzureCredentialEndpoint != ""
}

func newAzureSASBrokerPolicy(p MinioClientParam) (policy.Policy, error) {
	if p.AzureClientID == "" || p.AzureTenantID == "" || p.AzureCredentialEndpoint == "" ||
		p.AK == "" || p.Region == "" || p.BucketName == "" {
		return nil, errors.New(
			"azure credential broker requires client ID, tenant ID, credential endpoint, " +
				"account name, region, and container",
		)
	}
	if p.SK != "" || p.UseIAM || p.Anonymous || p.RoleARN != "" {
		return nil, errors.New("azure credential broker cannot be combined with another credential mode")
	}
	endpoint, err := url.Parse(p.AzureCredentialEndpoint)
	if err != nil || endpoint.Host == "" || (endpoint.Scheme != "http" && endpoint.Scheme != "https") {
		return nil, errors.New("azure credential endpoint must be a valid HTTP(S) URL")
	}

	durationSeconds := p.LoadFrequency
	if durationSeconds <= 0 {
		durationSeconds = defaultAzureSASDuration
	}
	requestTimeout := time.Duration(p.AzureRequestTimeoutMs) * time.Millisecond
	if requestTimeout <= 0 {
		requestTimeout = defaultAzureSASRequestTimeout
	}
	broker := &azureSASBroker{
		config: azureSASBrokerConfig{
			endpoint:        p.AzureCredentialEndpoint,
			region:          p.Region,
			bucket:          p.BucketName,
			clientID:        p.AzureClientID,
			tenantID:        p.AzureTenantID,
			accountName:     p.AK,
			durationSeconds: durationSeconds,
		},
		httpClient: &http.Client{Timeout: requestTimeout},
		now:        time.Now,
	}
	return &azureSASBrokerPolicy{broker: broker}, nil
}

func (p *azureSASBrokerPolicy) Do(request *policy.Request) (*http.Response, error) {
	token, err := p.broker.getToken(request.Raw().Context())
	if err != nil {
		return nil, err
	}
	requestURL := request.Raw().URL
	if requestURL.RawQuery == "" {
		requestURL.RawQuery = token
	} else {
		requestURL.RawQuery += "&" + token
	}
	return request.Next()
}

func newAzureRedactingTransport() policy.Transporter {
	return &azureRedactingTransport{client: &http.Client{}}
}

func (t *azureRedactingTransport) Do(request *http.Request) (*http.Response, error) {
	response, err := t.client.Do(request)
	if err == nil {
		return response, nil
	}

	var urlError *url.Error
	if !errors.As(err, &urlError) {
		return response, err
	}
	sanitized := *urlError
	requestURL, parseErr := url.Parse(sanitized.URL)
	if parseErr != nil {
		sanitized.URL = "<redacted>"
	} else {
		requestURL.RawQuery = ""
		requestURL.ForceQuery = false
		sanitized.URL = requestURL.String()
	}
	return response, &sanitized
}

func (b *azureSASBroker) getToken(ctx context.Context) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	now := b.now()
	if b.token != "" && b.expiresAt.Sub(now) > azureSASRefreshOffset {
		return b.token, nil
	}
	token, expiresAt, err := b.fetchToken(ctx, now)
	if err == nil {
		b.token = token
		b.expiresAt = expiresAt
		return token, nil
	}
	if b.token != "" {
		expired := !b.expiresAt.After(now)
		mlog.Warn(ctx, "Azure credential broker refresh failed",
			zap.Bool("cached_token_expired", expired),
			zap.Error(err),
		)
		if expired {
			return "", errors.Wrap(err, "refresh azure credential after cached token expired")
		}
		return b.token, nil
	}
	return "", err
}

func (b *azureSASBroker) fetchToken(ctx context.Context, now time.Time) (string, time.Time, error) {
	payload, err := json.Marshal(azureSASBrokerRequest{
		CSP:              "azure",
		Region:           b.config.region,
		Bucket:           b.config.bucket,
		DurationSeconds:  b.config.durationSeconds,
		AzureClientID:    b.config.clientID,
		AzureTenantID:    b.config.tenantID,
		AzureAccountName: b.config.accountName,
	})
	if err != nil {
		return "", time.Time{}, errors.Wrap(err, "marshal azure credential broker request")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, b.config.endpoint, bytes.NewReader(payload))
	if err != nil {
		return "", time.Time{}, errors.Wrap(err, "create azure credential broker request")
	}
	request.Header.Set("Content-Type", "application/json")

	response, err := b.httpClient.Do(request)
	if err != nil {
		return "", time.Time{}, errors.Wrap(err, "request azure credential broker")
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return "", time.Time{}, errors.Newf(
			"azure credential broker returned HTTP status %d", response.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, maxAzureSASBrokerResponseSize+1))
	if err != nil {
		return "", time.Time{}, errors.Wrap(err, "read azure credential broker response")
	}
	if len(body) > maxAzureSASBrokerResponseSize {
		return "", time.Time{}, errors.New("azure credential broker response exceeds 1 MiB")
	}

	var result azureSASBrokerResponse
	if err := json.Unmarshal(body, &result); err != nil {
		return "", time.Time{}, errors.Wrap(err, "parse azure credential broker response")
	}
	if !result.Success {
		return "", time.Time{}, errors.New("azure credential broker returned success=false")
	}
	if result.Credentials.AccountName != b.config.accountName {
		return "", time.Time{}, errors.Newf(
			"azure credential broker returned token for account %q, expected %q",
			result.Credentials.AccountName,
			b.config.accountName,
		)
	}
	token := strings.TrimPrefix(strings.TrimSpace(result.Credentials.SessionToken), "?")
	query, err := url.ParseQuery(token)
	if err != nil || query.Get("sig") == "" {
		return "", time.Time{}, errors.New("azure credential broker returned an invalid SAS token")
	}
	expiresAt, err := time.Parse(time.RFC3339, result.Credentials.ExpiredAt)
	if err != nil {
		return "", time.Time{}, errors.Wrap(err, "parse azure credential broker token expiration")
	}
	if !expiresAt.After(now) {
		return "", time.Time{}, errors.New("azure credential broker returned an expired SAS token")
	}
	return token, expiresAt, nil
}
