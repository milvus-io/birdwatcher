package oss

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
)

func TestAzureObjectStore(t *testing.T) {
	content := []byte("0123456789abcdef")
	lastModified := time.Date(2026, time.September, 2, 8, 0, 0, 0, time.UTC)
	server := newAzureObjectStoreTestServer(t, content, lastModified)
	defer server.Close()

	client, err := service.NewClientWithNoCredential(server.URL+"/", nil)
	if err != nil {
		t.Fatalf("create azure test client: %v", err)
	}
	store := &azureObjectStore{client: client, container: "container"}

	t.Run("open full object", func(t *testing.T) {
		reader, err := store.Open(t.Context(), "prefix/blob.bin")
		if err != nil {
			t.Fatalf("Open() error = %v", err)
		}
		closer := reader.(io.Closer)
		defer closer.Close()

		actual, err := io.ReadAll(reader)
		if err != nil {
			t.Fatalf("ReadAll() error = %v", err)
		}
		if !bytes.Equal(actual, content) {
			t.Fatalf("ReadAll() = %q, want %q", actual, content)
		}

		position, err := reader.Seek(-4, io.SeekEnd)
		if err != nil {
			t.Fatalf("Seek() error = %v", err)
		}
		if position != int64(len(content)-4) {
			t.Fatalf("Seek() = %d, want %d", position, len(content)-4)
		}
		actual = make([]byte, 4)
		if _, err := io.ReadFull(reader, actual); err != nil {
			t.Fatalf("read suffix: %v", err)
		}
		if !bytes.Equal(actual, content[len(content)-4:]) {
			t.Fatalf("suffix = %q, want %q", actual, content[len(content)-4:])
		}
	})

	t.Run("open range", func(t *testing.T) {
		reader, err := store.Open(t.Context(), "prefix/blob.bin", WithOpenRange(3, 7))
		if err != nil {
			t.Fatalf("Open() error = %v", err)
		}
		defer reader.(io.Closer).Close()

		actual, err := io.ReadAll(reader)
		if err != nil {
			t.Fatalf("ReadAll() error = %v", err)
		}
		if !bytes.Equal(actual, content[3:8]) {
			t.Fatalf("range = %q, want %q", actual, content[3:8])
		}

		actual = make([]byte, 3)
		if _, err := reader.ReadAt(actual, 1); err != nil {
			t.Fatalf("ReadAt() error = %v", err)
		}
		if !bytes.Equal(actual, content[4:7]) {
			t.Fatalf("ReadAt() = %q, want %q", actual, content[4:7])
		}
	})

	t.Run("invalid range", func(t *testing.T) {
		if _, err := store.Open(t.Context(), "prefix/blob.bin", WithOpenRange(4, 3)); err == nil {
			t.Fatal("Open() expected invalid range error")
		}
	})

	t.Run("stat", func(t *testing.T) {
		stat, err := store.Stat(t.Context(), "prefix/blob.bin")
		if err != nil {
			t.Fatalf("Stat() error = %v", err)
		}
		if stat.Size != int64(len(content)) || stat.ETag != `"etag"` || !stat.LastModified.Equal(lastModified) || stat.VersionID != "version-1" {
			t.Fatalf("Stat() = %#v", stat)
		}
	})

	t.Run("list recursive", func(t *testing.T) {
		objects, err := store.List(t.Context(), "prefix/", true)
		if err != nil {
			t.Fatalf("List() error = %v", err)
		}
		items := collectAzureObjectInfos(objects)
		if len(items) != 1 || items[0].Key != "prefix/blob.bin" || items[0].Size != int64(len(content)) || items[0].IsDir {
			t.Fatalf("List() = %#v", items)
		}
	})

	t.Run("list hierarchy", func(t *testing.T) {
		objects, err := store.List(t.Context(), "prefix/", false)
		if err != nil {
			t.Fatalf("List() error = %v", err)
		}
		items := collectAzureObjectInfos(objects)
		if len(items) != 2 || items[0].Key != "prefix/blob.bin" || items[1].Key != "prefix/dir/" || !items[1].IsDir {
			t.Fatalf("List() = %#v", items)
		}
	})
}

func TestNewAzureObjectStore(t *testing.T) {
	t.Run("empty container", func(t *testing.T) {
		_, err := NewAzureObjectStore(t.Context(), MinioClientParam{})
		if err == nil || !strings.Contains(err.Error(), "container name is empty") {
			t.Fatalf("NewAzureObjectStore() error = %v", err)
		}
	})

	t.Run("container check", func(t *testing.T) {
		checked := make(chan struct{}, 1)
		server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
			if request.URL.Query().Get("restype") != "container" {
				t.Errorf("unexpected azure request: %s", request.URL.String())
				response.WriteHeader(http.StatusBadRequest)
				return
			}
			select {
			case checked <- struct{}{}:
			default:
			}
			response.Header().Set("x-ms-request-id", "request-id")
			response.Header().Set("x-ms-version", "2021-12-02")
			response.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		accountKey := base64.StdEncoding.EncodeToString(make([]byte, 32))
		t.Setenv("AZURE_STORAGE_CONNECTION_STRING", fmt.Sprintf(
			"DefaultEndpointsProtocol=http;AccountName=test;AccountKey=%s;BlobEndpoint=%s/test;",
			accountKey,
			server.URL,
		))
		store, err := NewAzureObjectStore(t.Context(), MinioClientParam{BucketName: "container"})
		if err != nil {
			t.Fatalf("NewAzureObjectStore() error = %v", err)
		}
		if store == nil {
			t.Fatal("NewAzureObjectStore() returned nil store")
		}
		select {
		case <-checked:
		default:
			t.Fatal("NewAzureObjectStore() did not check the container")
		}
	})

	t.Run("skip container check", func(t *testing.T) {
		requested := make(chan struct{}, 1)
		server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
			select {
			case requested <- struct{}{}:
			default:
			}
			response.WriteHeader(http.StatusInternalServerError)
		}))
		defer server.Close()

		accountKey := base64.StdEncoding.EncodeToString(make([]byte, 32))
		t.Setenv("AZURE_STORAGE_CONNECTION_STRING", fmt.Sprintf(
			"DefaultEndpointsProtocol=http;AccountName=test;AccountKey=%s;BlobEndpoint=%s/test;",
			accountKey,
			server.URL,
		))
		param := MinioClientParam{BucketName: "container"}
		WithSkipCheckBucket(true)(&param)
		store, err := NewAzureObjectStore(t.Context(), param)
		if err != nil {
			t.Fatalf("NewAzureObjectStore() error = %v", err)
		}
		if store == nil {
			t.Fatal("NewAzureObjectStore() returned nil store")
		}
		select {
		case <-requested:
			t.Fatal("skip bucket check issued an HTTP request")
		default:
		}
	})
}

func TestAzureServiceURL(t *testing.T) {
	url, err := azureServiceURL(MinioClientParam{
		AK:     "account",
		Addr:   "core.windows.net/",
		UseSSL: true,
	})
	if err != nil {
		t.Fatalf("azureServiceURL() error = %v", err)
	}
	if url != "https://account.blob.core.windows.net/" {
		t.Fatalf("azureServiceURL() = %q", url)
	}
	url, err = azureServiceURL(MinioClientParam{
		AK:     "account",
		Addr:   "account.blob.core.windows.net",
		UseSSL: true,
	})
	if err != nil {
		t.Fatalf("azureServiceURL() with account-qualified host error = %v", err)
	}
	if url != "https://account.blob.core.windows.net/" {
		t.Fatalf("azureServiceURL() with account-qualified host = %q", url)
	}
	url, err = azureServiceURL(MinioClientParam{
		Addr:   "account.blob.core.windows.net",
		UseSSL: true,
	})
	if err != nil {
		t.Fatalf("azureServiceURL() without explicit account error = %v", err)
	}
	if url != "https://account.blob.core.windows.net/" {
		t.Fatalf("azureServiceURL() without explicit account = %q", url)
	}
	url, err = azureServiceURL(MinioClientParam{
		AK:   "account",
		Addr: "core.windows.net",
	})
	if err != nil {
		t.Fatalf("azureServiceURL() with HTTP error = %v", err)
	}
	if url != "http://account.blob.core.windows.net/" {
		t.Fatalf("azureServiceURL() with HTTP = %q", url)
	}
	url, err = azureServiceURL(MinioClientParam{Addr: "account.blob.core.windows.net"})
	if err != nil {
		t.Fatalf("azureServiceURL() with account-qualified HTTP error = %v", err)
	}
	if url != "http://account.blob.core.windows.net/" {
		t.Fatalf("azureServiceURL() with account-qualified HTTP = %q", url)
	}
	if _, err := azureServiceURL(MinioClientParam{Addr: "core.windows.net"}); err == nil {
		t.Fatal("azureServiceURL() expected account error")
	}
	if _, err := azureServiceURL(MinioClientParam{
		AK:   "other-account",
		Addr: "account.blob.core.windows.net",
	}); err == nil {
		t.Fatal("azureServiceURL() expected account mismatch error")
	}
	if _, err := azureServiceURL(MinioClientParam{AK: "account"}); err == nil {
		t.Fatal("azureServiceURL() expected endpoint error")
	}

	accountKey := base64.StdEncoding.EncodeToString(make([]byte, 32))
	t.Setenv("AZURE_STORAGE_CONNECTION_STRING",
		"DefaultEndpointsProtocol=https;AccountName=ambientaccount;AccountKey="+
			accountKey+";EndpointSuffix=core.windows.net")
	client, err := newAzureServiceClient(MinioClientParam{
		AK:                           "requestaccount",
		SK:                           accountKey,
		Addr:                         "core.windows.net",
		UseSSL:                       true,
		DisableAzureConnectionString: true,
	})
	if err != nil {
		t.Fatalf("newAzureServiceClient() error = %v", err)
	}
	if client.URL() != "https://requestaccount.blob.core.windows.net/" {
		t.Fatalf("newAzureServiceClient() URL = %q", client.URL())
	}
}

func TestAzureRedactingTransportRedactsQueryFromErrors(t *testing.T) {
	transport := &azureRedactingTransport{client: &http.Client{
		Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return nil, fmt.Errorf("dial failed")
		}),
	}}
	request, err := http.NewRequestWithContext(
		t.Context(),
		http.MethodGet,
		"https://account.blob.core.windows.net/container/blob?sv=1&sig=secret-signature",
		nil,
	)
	if err != nil {
		t.Fatalf("create request: %v", err)
	}
	_, err = transport.Do(request)
	if err == nil {
		t.Fatal("azureRedactingTransport.Do() expected an error")
	}
	if strings.Contains(err.Error(), "secret-signature") || strings.Contains(err.Error(), "sig=") {
		t.Fatalf("azureRedactingTransport.Do() leaked SAS query: %v", err)
	}
	if !strings.Contains(err.Error(), "account.blob.core.windows.net/container/blob") {
		t.Fatalf("azureRedactingTransport.Do() removed the useful object path: %v", err)
	}
}

func TestAzureSASBrokerRefreshFallback(t *testing.T) {
	now := time.Date(2026, time.September, 3, 12, 0, 0, 0, time.UTC)
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	newBroker := func(expiresAt time.Time) *azureSASBroker {
		return &azureSASBroker{
			config:     azureSASBrokerConfig{endpoint: server.URL},
			httpClient: server.Client(),
			now: func() time.Time {
				return now
			},
			token:     "sv=1&sig=cached",
			expiresAt: expiresAt,
		}
	}

	t.Run("still valid", func(t *testing.T) {
		broker := newBroker(now.Add(30 * time.Second))
		token, err := broker.getToken(t.Context())
		if err != nil {
			t.Fatalf("getToken() error = %v", err)
		}
		if token != "sv=1&sig=cached" {
			t.Fatalf("getToken() = %q", token)
		}
	})

	t.Run("expired", func(t *testing.T) {
		broker := newBroker(now.Add(-time.Second))
		token, err := broker.getToken(t.Context())
		if err == nil || !strings.Contains(err.Error(), "cached token expired") {
			t.Fatalf("getToken() token = %q, error = %v", token, err)
		}
		if token != "" {
			t.Fatalf("getToken() returned expired token %q", token)
		}
	})
}

func TestAzureSASBrokerPolicy(t *testing.T) {
	var brokerRequests int
	var brokerRequest azureSASBrokerRequest
	broker := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		brokerRequests++
		if request.Method != http.MethodPost || request.Header.Get("Content-Type") != "application/json" {
			t.Errorf("unexpected broker request: method=%s content-type=%s", request.Method, request.Header.Get("Content-Type"))
		}
		if err := json.NewDecoder(request.Body).Decode(&brokerRequest); err != nil {
			t.Errorf("decode broker request: %v", err)
		}
		response.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(response).Encode(map[string]any{
			"success": true,
			"credentials": map[string]string{
				"tempAk":       "account",
				"sessionToken": "?sv=1&sig=encoded%2Bsignature%3D",
				"expiredAt":    time.Now().Add(time.Hour).UTC().Format(time.RFC3339),
			},
		})
	}))
	defer broker.Close()

	brokerPolicy, err := newAzureSASBrokerPolicy(MinioClientParam{
		AK:                      "account",
		Region:                  "westus3",
		BucketName:              "container",
		AzureClientID:           "client-id",
		AzureTenantID:           "tenant-id",
		AzureCredentialEndpoint: broker.URL,
		LoadFrequency:           3600,
	})
	if err != nil {
		t.Fatalf("newAzureSASBrokerPolicy() error = %v", err)
	}

	var storageRequests int
	storage := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		storageRequests++
		query := request.URL.Query()
		if query.Get("restype") != "container" || query.Get("sig") != "encoded+signature=" {
			t.Errorf("storage query = %q", request.URL.RawQuery)
		}
		response.Header().Set("x-ms-request-id", "request-id")
		response.Header().Set("x-ms-version", "2021-12-02")
		response.WriteHeader(http.StatusOK)
	}))
	defer storage.Close()

	options := &service.ClientOptions{}
	options.PerCallPolicies = append(options.PerCallPolicies, brokerPolicy)
	client, err := service.NewClientWithNoCredential(storage.URL+"/", options)
	if err != nil {
		t.Fatalf("create azure test client: %v", err)
	}
	for range 2 {
		if _, err := client.NewContainerClient("container").GetProperties(t.Context(), nil); err != nil {
			t.Fatalf("GetProperties() error = %v", err)
		}
	}
	if brokerRequests != 1 || storageRequests != 2 {
		t.Fatalf("request counts: broker=%d storage=%d", brokerRequests, storageRequests)
	}
	if brokerRequest.CSP != "azure" || brokerRequest.Region != "westus3" ||
		brokerRequest.Bucket != "container" || brokerRequest.DurationSeconds != 3600 ||
		brokerRequest.AzureClientID != "client-id" || brokerRequest.AzureTenantID != "tenant-id" ||
		brokerRequest.AzureAccountName != "account" {
		t.Fatalf("broker request = %#v", brokerRequest)
	}
}

func TestAzureSASBrokerPolicyRejectsIncompleteConfig(t *testing.T) {
	_, err := newAzureSASBrokerPolicy(MinioClientParam{
		AK:                      "account",
		Region:                  "westus3",
		BucketName:              "container",
		AzureClientID:           "client-id",
		AzureCredentialEndpoint: "https://broker.example.com",
	})
	if err == nil || !strings.Contains(err.Error(), "requires client ID") {
		t.Fatalf("newAzureSASBrokerPolicy() error = %v", err)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func newAzureObjectStoreTestServer(t *testing.T, content []byte, lastModified time.Time) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set("x-ms-request-id", "request-id")
		response.Header().Set("x-ms-version", "2021-12-02")

		if request.URL.Query().Get("comp") == "list" {
			response.Header().Set("Content-Type", "application/xml")
			hierarchy := request.URL.Query().Get("delimiter") == "/"
			prefix := ""
			if hierarchy {
				prefix = "<BlobPrefix><Name>prefix/dir/</Name></BlobPrefix>"
			}
			_, _ = fmt.Fprintf(response, `<?xml version="1.0" encoding="utf-8"?>
<EnumerationResults ServiceEndpoint="%s" ContainerName="container">
  <Prefix>prefix/</Prefix><Marker></Marker><MaxResults>5000</MaxResults>
  <Blobs>
    <Blob>
      <Name>prefix/blob.bin</Name><Deleted>false</Deleted><Snapshot></Snapshot><VersionId>version-1</VersionId>
      <Properties><Last-Modified>%s</Last-Modified><Etag>"etag"</Etag><Content-Length>%d</Content-Length><BlobType>BlockBlob</BlobType></Properties>
    </Blob>
    %s
  </Blobs>
  <NextMarker></NextMarker>
</EnumerationResults>`, request.URL.Scheme+request.URL.Host, lastModified.Format(http.TimeFormat), len(content), prefix)
			return
		}

		if request.Method == http.MethodHead {
			response.Header().Set("Content-Length", strconv.Itoa(len(content)))
			response.Header().Set("ETag", `"etag"`)
			response.Header().Set("Last-Modified", lastModified.Format(http.TimeFormat))
			response.Header().Set("x-ms-version-id", "version-1")
			response.WriteHeader(http.StatusOK)
			return
		}

		rangeHeader := request.Header.Get("x-ms-range")
		if rangeHeader == "" {
			rangeHeader = request.Header.Get("Range")
		}
		start, end := parseAzureTestRange(t, rangeHeader, int64(len(content)))
		response.Header().Set("Content-Length", strconv.FormatInt(end-start+1, 10))
		response.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(content)))
		response.Header().Set("ETag", `"etag"`)
		response.Header().Set("Last-Modified", lastModified.Format(http.TimeFormat))
		response.WriteHeader(http.StatusPartialContent)
		_, _ = response.Write(content[start : end+1])
	}))
}

func parseAzureTestRange(t *testing.T, value string, size int64) (int64, int64) {
	t.Helper()
	if value == "" {
		return 0, size - 1
	}
	value = strings.TrimPrefix(value, "bytes=")
	parts := strings.SplitN(value, "-", 2)
	start, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		t.Fatalf("invalid range start %q: %v", value, err)
	}
	end := size - 1
	if len(parts) == 2 && parts[1] != "" {
		end, err = strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			t.Fatalf("invalid range end %q: %v", value, err)
		}
	}
	return start, end
}

func collectAzureObjectInfos(source <-chan ObjectInfo) []ObjectInfo {
	var result []ObjectInfo
	for item := range source {
		result = append(result, item)
	}
	return result
}
