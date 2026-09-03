package oss

import (
	"context"
	"fmt"
	"io"
	"math"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"github.com/cockroachdb/errors"

	"github.com/milvus-io/birdwatcher/models"
	storagecommon "github.com/milvus-io/birdwatcher/storage/common"
)

type azureObjectStore struct {
	client    *service.Client
	container string
}

// NewAzureObjectStore creates an Azure Blob-backed ObjectStore.
func NewAzureObjectStore(ctx context.Context, p MinioClientParam) (ObjectStore, error) {
	if strings.TrimSpace(p.BucketName) == "" {
		return nil, errors.New("azure container name is empty")
	}

	client, err := newAzureServiceClient(p)
	if err != nil {
		return nil, err
	}
	if !p.skipCheckBucket {
		_, err = client.NewContainerClient(p.BucketName).GetProperties(ctx, nil)
		if err != nil {
			return nil, errors.Wrapf(err, "check azure container %s", p.BucketName)
		}
	}

	return &azureObjectStore{client: client, container: p.BucketName}, nil
}

func newAzureServiceClient(p MinioClientParam) (*service.Client, error) {
	options := &service.ClientOptions{}
	options.Transport = newAzureRedactingTransport()
	if hasAzureSASBrokerConfig(p) {
		brokerPolicy, err := newAzureSASBrokerPolicy(p)
		if err != nil {
			return nil, err
		}
		serviceURL, err := azureServiceURL(p)
		if err != nil {
			return nil, err
		}
		options.PerCallPolicies = append(options.PerCallPolicies, brokerPolicy)
		return service.NewClientWithNoCredential(serviceURL, options)
	}
	if !p.Anonymous && !p.UseIAM && !p.DisableAzureConnectionString {
		if connectionString := os.Getenv("AZURE_STORAGE_CONNECTION_STRING"); connectionString != "" {
			return service.NewClientFromConnectionString(connectionString, options)
		}
	}
	serviceURL, err := azureServiceURL(p)
	if err != nil {
		return nil, err
	}

	if p.Anonymous {
		return service.NewClientWithNoCredential(serviceURL, options)
	}
	if p.UseIAM {
		if tokenFile := os.Getenv("AZURE_FEDERATED_TOKEN_FILE"); tokenFile != "" {
			credential, err := azidentity.NewWorkloadIdentityCredential(&azidentity.WorkloadIdentityCredentialOptions{
				ClientID:      os.Getenv("AZURE_CLIENT_ID"),
				TenantID:      os.Getenv("AZURE_TENANT_ID"),
				TokenFilePath: tokenFile,
			})
			if err != nil {
				return nil, errors.Wrap(err, "create azure workload identity credential")
			}
			return service.NewClient(serviceURL, credential, options)
		}

		identityID := azidentity.ClientID("")
		if clientID := os.Getenv("AZURE_CLIENT_ID"); clientID != "" {
			identityID = azidentity.ClientID(clientID)
		}
		credential, err := azidentity.NewManagedIdentityCredential(&azidentity.ManagedIdentityCredentialOptions{ID: identityID})
		if err != nil {
			return nil, errors.Wrap(err, "create azure managed identity credential")
		}
		return service.NewClient(serviceURL, credential, options)
	}

	if p.AK == "" || p.SK == "" {
		return nil, errors.New("azure storage account name and key are required")
	}
	credential, err := azblob.NewSharedKeyCredential(p.AK, p.SK)
	if err != nil {
		return nil, errors.Wrap(err, "create azure shared key credential")
	}
	return service.NewClientWithSharedKeyCredential(serviceURL, credential, options)
}

func azureServiceURL(p MinioClientParam) (string, error) {
	address := strings.Trim(strings.TrimSpace(p.Addr), "/")
	if address == "" {
		return "", errors.New("azure storage endpoint is empty")
	}
	if err := validateAzureServiceHost(address); err != nil {
		return "", err
	}

	accountFromHost, accountQualified := azureAccountFromServiceHost(address)
	scheme := "http"
	if p.UseSSL {
		scheme = "https"
	}
	if accountQualified {
		if p.AK != "" && !strings.EqualFold(strings.TrimSpace(p.AK), accountFromHost) {
			return "", errors.New("azure storage account name does not match the service endpoint")
		}
		return scheme + "://" + address + "/", nil
	}

	accountName := strings.TrimSpace(p.AK)
	if accountName == "" {
		return "", errors.New(
			"azure storage account name is required when address is an endpoint suffix",
		)
	}
	serviceHost := fmt.Sprintf("%s.blob.%s", accountName, address)
	if err := validateAzureServiceHost(serviceHost); err != nil {
		return "", err
	}
	return scheme + "://" + serviceHost + "/", nil
}

func validateAzureServiceHost(host string) error {
	u, err := url.Parse("https://" + host + "/")
	if err != nil || u.User != nil || u.Host != host || u.Path != "/" ||
		u.RawQuery != "" || u.Fragment != "" {
		return errors.New("azure storage endpoint must be a bare host")
	}
	return nil
}

func azureAccountFromServiceHost(host string) (string, bool) {
	hostname := strings.ToLower(host)
	if u, err := url.Parse("https://" + host); err == nil {
		hostname = strings.ToLower(u.Hostname())
	}
	markerIndex := strings.Index(hostname, ".blob.")
	if markerIndex <= 0 {
		return "", false
	}
	accountName := strings.SplitN(hostname[:markerIndex], ".", 2)[0]
	return accountName, accountName != ""
}

func (s *azureObjectStore) Open(ctx context.Context, key string, opts ...OpenOption) (storagecommon.ReadSeeker, error) {
	settings := &openSettings{}
	for _, opt := range opts {
		if opt != nil {
			opt(settings)
		}
	}

	start := int64(0)
	length := int64(-1)
	if settings.rangeSet {
		if settings.start < 0 || settings.end < settings.start {
			return nil, errors.New("invalid open range")
		}
		if settings.end-settings.start == math.MaxInt64 {
			return nil, errors.New("open range is too large")
		}
		start = settings.start
		length = settings.end - settings.start + 1
	}

	client := s.client.NewContainerClient(s.container).NewBlobClient(key)
	return &azureBlobReader{
		ctx:    ctx,
		client: client,
		start:  start,
		length: length,
	}, nil
}

func (s *azureObjectStore) Stat(ctx context.Context, key string) (*models.FsStat, error) {
	response, err := s.client.NewContainerClient(s.container).NewBlobClient(key).GetProperties(ctx, nil)
	if err != nil {
		return nil, err
	}
	if response.ContentLength == nil {
		return nil, errors.New("azure blob response is missing content length")
	}

	result := &models.FsStat{Size: *response.ContentLength}
	if response.ETag != nil {
		result.ETag = string(*response.ETag)
	}
	if response.LastModified != nil {
		result.LastModified = *response.LastModified
	}
	if response.VersionID != nil {
		result.VersionID = *response.VersionID
	}
	return result, nil
}

func (s *azureObjectStore) List(ctx context.Context, prefix string, recursive bool) (<-chan ObjectInfo, error) {
	result := make(chan ObjectInfo)
	containerClient := s.client.NewContainerClient(s.container)
	go func() {
		defer close(result)
		if recursive {
			pager := containerClient.NewListBlobsFlatPager(&azblob.ListBlobsFlatOptions{Prefix: &prefix})
			for pager.More() {
				response, err := pager.NextPage(ctx)
				if err != nil {
					sendAzureObjectInfo(ctx, result, ObjectInfo{Err: err})
					return
				}
				if response.Segment == nil {
					sendAzureObjectInfo(ctx, result, ObjectInfo{Err: errors.New("azure list response is missing segment")})
					return
				}
				for _, item := range response.Segment.BlobItems {
					if item == nil || item.Name == nil {
						continue
					}
					if item.Properties == nil {
						continue
					}
					if !sendAzureObjectInfo(ctx, result, azureBlobItemInfo(
						item.Name,
						item.Properties.ContentLength,
						item.Properties.ETag,
						item.Properties.LastModified,
						item.VersionID,
					)) {
						return
					}
				}
			}
			return
		}

		pager := containerClient.NewListBlobsHierarchyPager("/", &container.ListBlobsHierarchyOptions{Prefix: &prefix})
		for pager.More() {
			response, err := pager.NextPage(ctx)
			if err != nil {
				sendAzureObjectInfo(ctx, result, ObjectInfo{Err: err})
				return
			}
			if response.Segment == nil {
				sendAzureObjectInfo(ctx, result, ObjectInfo{Err: errors.New("azure list response is missing segment")})
				return
			}
			for _, item := range response.Segment.BlobItems {
				if item == nil || item.Name == nil {
					continue
				}
				if item.Properties == nil {
					continue
				}
				if !sendAzureObjectInfo(ctx, result, azureBlobItemInfo(
					item.Name,
					item.Properties.ContentLength,
					item.Properties.ETag,
					item.Properties.LastModified,
					item.VersionID,
				)) {
					return
				}
			}
			for _, item := range response.Segment.BlobPrefixes {
				if item == nil || item.Name == nil {
					continue
				}
				if !sendAzureObjectInfo(ctx, result, ObjectInfo{Key: *item.Name, IsDir: true}) {
					return
				}
			}
		}
	}()
	return result, nil
}

func azureBlobItemInfo(
	name *string,
	contentLength *int64,
	etag *azcore.ETag,
	lastModified *time.Time,
	versionID *string,
) ObjectInfo {
	result := ObjectInfo{Key: *name}
	if contentLength != nil {
		result.Size = *contentLength
	}
	if etag != nil {
		result.ETag = string(*etag)
	}
	if lastModified != nil {
		result.LastModified = *lastModified
	}
	if versionID != nil {
		result.VersionID = *versionID
	}
	return result
}

func sendAzureObjectInfo(ctx context.Context, ch chan<- ObjectInfo, info ObjectInfo) bool {
	select {
	case ch <- info:
		return true
	case <-ctx.Done():
		return false
	}
}

type azureBlobReader struct {
	ctx    context.Context
	client *blob.Client
	start  int64
	length int64
	pos    int64
	body   io.ReadCloser
}

func (r *azureBlobReader) Read(buffer []byte) (int, error) {
	if len(buffer) == 0 {
		return 0, nil
	}
	if r.length >= 0 && r.pos >= r.length {
		return 0, io.EOF
	}
	if r.body == nil {
		if err := r.openBody(); err != nil {
			return 0, err
		}
	}

	if r.length >= 0 {
		remaining := r.length - r.pos
		if int64(len(buffer)) > remaining {
			buffer = buffer[:remaining]
		}
	}
	n, err := r.body.Read(buffer)
	r.pos += int64(n)
	if err == io.EOF {
		_ = r.closeBody()
	}
	return n, err
}

func (r *azureBlobReader) ReadAt(buffer []byte, offset int64) (int, error) {
	if offset < 0 {
		return 0, errors.New("negative azure blob read offset")
	}
	if len(buffer) == 0 {
		return 0, nil
	}
	if r.length >= 0 && offset >= r.length {
		return 0, io.EOF
	}

	readLength := int64(len(buffer))
	if offset > math.MaxInt64-readLength {
		return 0, errors.New("azure blob read range overflows int64")
	}
	if r.length >= 0 && offset+readLength > r.length {
		readLength = r.length - offset
	}
	response, err := r.client.DownloadStream(r.ctx, &blob.DownloadStreamOptions{
		Range: blob.HTTPRange{Offset: r.start + offset, Count: readLength},
	})
	if err != nil {
		return 0, err
	}
	body := response.NewRetryReader(r.ctx, nil)
	defer body.Close()

	n, err := io.ReadFull(body, buffer[:readLength])
	if err != nil {
		return n, err
	}
	if readLength < int64(len(buffer)) {
		return n, io.EOF
	}
	return n, nil
}

func (r *azureBlobReader) Seek(offset int64, whence int) (int64, error) {
	var next int64
	switch whence {
	case io.SeekStart:
		next = offset
	case io.SeekCurrent:
		next = r.pos + offset
	case io.SeekEnd:
		if r.length < 0 {
			response, err := r.client.GetProperties(r.ctx, nil)
			if err != nil {
				return 0, err
			}
			if response.ContentLength == nil || *response.ContentLength < r.start {
				return 0, errors.New("invalid azure blob content length")
			}
			r.length = *response.ContentLength - r.start
		}
		next = r.length + offset
	default:
		return 0, errors.New("invalid seek whence")
	}
	if next < 0 {
		return 0, errors.New("negative azure blob seek position")
	}
	if err := r.closeBody(); err != nil {
		return 0, err
	}
	r.pos = next
	return next, nil
}

func (r *azureBlobReader) Close() error {
	return r.closeBody()
}

func (r *azureBlobReader) openBody() error {
	httpRange := blob.HTTPRange{Offset: r.start + r.pos}
	if r.length >= 0 {
		httpRange.Count = r.length - r.pos
	}
	response, err := r.client.DownloadStream(r.ctx, &blob.DownloadStreamOptions{Range: httpRange})
	if err != nil {
		return err
	}
	r.body = response.NewRetryReader(r.ctx, nil)
	return nil
}

func (r *azureBlobReader) closeBody() error {
	if r.body == nil {
		return nil
	}
	err := r.body.Close()
	r.body = nil
	return err
}
