package ossutil

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"path"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/oss"
)

// ExternalSourceSpec is the parsed external collection specification
// (schema.ExternalSpec). extfs keys mirror milvus
// pkg/util/externalspec/specutil.ExtfsKey*.
type ExternalSourceSpec struct {
	Format             string
	CloudProvider      string
	Region             string
	RoleARN            string
	RoleSessionName    string
	ExternalID         string
	AliyunRoleAuthMode string
	AK                 string
	SK                 string
	UseIAM             bool
	IAMEndpoint        string
	UseSSL             *bool
	Anonymous          bool
	BucketName         string
	StorageType        string
	LoadFrequency      int
}

// LocationForm distinguishes the two accepted external_source URI shapes,
// mirroring milvus pkg/util/externalspec and milvus-storage StorageUri.
type LocationForm int

const (
	// LocationFormMilvus is `scheme://endpoint/bucket/key`: the URI host is the
	// storage endpoint and the first path segment is the bucket.
	LocationFormMilvus LocationForm = iota
	// LocationFormAWS is `scheme://bucket/key`: the URI host is the bucket and
	// the endpoint is derived from extfs.cloud_provider + extfs.region.
	LocationFormAWS
)

// ExternalSourceLocation is the parsed external_source URI.
type ExternalSourceLocation struct {
	Scheme   string
	Host     string
	Bucket   string
	RootPath string
	// Address is the storage endpoint used to build the object store client:
	// the URI host for the Milvus form, or DeriveEndpoint(provider, region)
	// for the AWS form.
	Address string
	Form    LocationForm
}

// ParseExternalSpec parses the external spec JSON (schema.ExternalSpec).
func ParseExternalSpec(raw string) (ExternalSourceSpec, error) {
	return parseExternalSpec(raw, true)
}

// ParseExternalSpecLoose parses the external spec JSON without rejecting
// unsupported file formats. It is used by commands that only annotate paths
// (e.g. show manifest) and must not fail on non-parquet external collections.
func ParseExternalSpecLoose(raw string) (ExternalSourceSpec, error) {
	return parseExternalSpec(raw, false)
}

func parseExternalSpec(raw string, validateFormat bool) (ExternalSourceSpec, error) {
	if strings.TrimSpace(raw) == "" {
		return ExternalSourceSpec{}, nil
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		return ExternalSourceSpec{}, fmt.Errorf("parse external spec: %w", err)
	}

	spec := ExternalSourceSpec{
		Format:             ReadMapString(payload, "format"),
		AliyunRoleAuthMode: ReadMapString(payload, "aliyun_role_auth_mode"),
	}
	if validateFormat && spec.Format != "" && !strings.EqualFold(spec.Format, "parquet") {
		return ExternalSourceSpec{}, fmt.Errorf("external collection format %s is not supported", spec.Format)
	}

	extfs, _ := payload["extfs"].(map[string]any)
	if len(extfs) > 0 {
		spec.CloudProvider = ReadMapString(extfs, "cloud_provider")
		spec.Region = ReadMapString(extfs, "region")
		spec.RoleARN = ReadMapString(extfs, "role_arn")
		spec.RoleSessionName = ReadMapString(extfs, "session_name")
		spec.ExternalID = ReadMapString(extfs, "external_id")
		spec.AK = ReadMapString(extfs, "access_key_id")
		spec.SK = ReadMapString(extfs, "access_key_value")
		spec.IAMEndpoint = ReadMapString(extfs, "iam_endpoint")
		spec.BucketName = ReadMapString(extfs, "bucket_name")
		spec.StorageType = ReadMapString(extfs, "storage_type")
		if spec.AliyunRoleAuthMode == "" {
			spec.AliyunRoleAuthMode = ReadMapString(extfs, "aliyun_role_auth_mode")
		}
		if useIAM, ok := ReadMapBool(extfs, "use_iam"); ok {
			spec.UseIAM = useIAM
		}
		if anonymous, ok := ReadMapBool(extfs, "anonymous"); ok {
			spec.Anonymous = anonymous
		}
		if useSSL, ok := ReadMapBool(extfs, "use_ssl"); ok {
			spec.UseSSL = &useSSL
		}
		if lf := ReadMapString(extfs, "load_frequency"); lf != "" {
			v, err := strconv.Atoi(lf)
			if err != nil {
				return ExternalSourceSpec{}, fmt.Errorf("invalid extfs.load_frequency %q: %w", lf, err)
			}
			spec.LoadFrequency = v
		}
	}
	return spec, nil
}

// ParseExternalSource parses the external_source URI into scheme / host /
// bucket / root path, assuming the Milvus form (host = endpoint, first path
// segment = bucket). Use ResolveExternalSource to apply the AWS-form swap
// based on the spec.
func ParseExternalSource(raw string) (ExternalSourceLocation, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return ExternalSourceLocation{}, fmt.Errorf("parse external source: %w", err)
	}
	if u.Scheme == "" || u.Host == "" {
		return ExternalSourceLocation{}, fmt.Errorf("invalid external source: %s", raw)
	}
	parts := strings.Split(strings.Trim(strings.TrimSpace(u.Path), "/"), "/")
	if len(parts) == 0 || parts[0] == "" {
		return ExternalSourceLocation{}, fmt.Errorf("external source %s does not include bucket", raw)
	}
	location := ExternalSourceLocation{
		Scheme:   u.Scheme,
		Host:     u.Host,
		Bucket:   parts[0],
		RootPath: "",
		Address:  u.Host,
		Form:     LocationFormMilvus,
	}
	if len(parts) > 1 {
		location.RootPath = path.Clean(filepath.Join(parts[1:]...))
		if location.RootPath == "." {
			location.RootPath = ""
		}
	}
	return location, nil
}

// ResolveExternalSource applies the two-form URI contract on top of
// ParseExternalSource. It mirrors milvus-storage's Layer-3 swap decision:
//
//	derived := DeriveEndpoint(spec.CloudProvider, spec.Region)
//	if derived != "" && StripURIScheme(derived) != host &&
//	   !IsCloudEndpointHost(host) {
//	    // AWS form: host is the bucket, endpoint is derived
//	}
//
// The returned location carries the final bucket/root path and the endpoint
// address used to build the object store client.
func ResolveExternalSource(raw string, spec ExternalSourceSpec) (ExternalSourceLocation, error) {
	location, err := ParseExternalSource(raw)
	if err != nil {
		return ExternalSourceLocation{}, err
	}

	derived := DeriveEndpoint(spec.CloudProvider, spec.Region)
	if derived != "" && StripURIScheme(derived) != location.Host && !IsCloudEndpointHost(location.Host) {
		// AWS form: host is the bucket, path is the key (may be empty).
		location.Form = LocationFormAWS
		location.Bucket = location.Host
		trimmed := strings.TrimPrefix(strings.TrimSpace(raw), location.Scheme+"://"+location.Host)
		trimmed = strings.TrimPrefix(trimmed, "/")
		if trimmed != "" {
			location.RootPath = path.Clean(trimmed)
			if location.RootPath == "." {
				location.RootPath = ""
			}
		} else {
			location.RootPath = ""
		}
		location.Address = derived
	}
	return location, nil
}

// StripURIScheme removes an http(s):// prefix, if present.
func StripURIScheme(addr string) string {
	for _, scheme := range []string{"https://", "http://"} {
		if strings.HasPrefix(addr, scheme) {
			return strings.TrimPrefix(addr, scheme)
		}
	}
	return addr
}

// IsCloudEndpointHost reports whether host belongs to a known cloud provider
// endpoint family (i.e. the URI is Milvus-form regardless of the derived
// endpoint). Mirrors milvus externalspec.IsCloudEndpointHost.
func IsCloudEndpointHost(host string) bool {
	h := strings.ToLower(host)
	suffixes := []string{
		".amazonaws.com", ".amazonaws.com.cn",
		".googleapis.com",
		".aliyuncs.com",
		".myqcloud.com",
		".myhuaweicloud.com",
		".core.windows.net", ".core.chinacloudapi.cn",
		".core.usgovcloudapi.net", ".core.cloudapi.de",
	}
	for _, s := range suffixes {
		if strings.HasSuffix(h, s) {
			return true
		}
	}
	return false
}

// DeriveEndpoint returns the cloud endpoint for a provider/region pair, or ""
// when it cannot be derived (empty region, unknown provider). Mirrors milvus
// externalspec.DeriveEndpoint.
func DeriveEndpoint(cloudProvider, region string) string {
	cp := strings.ToLower(cloudProvider)
	switch cp {
	case "aws":
		if region == "" {
			return ""
		}
		if strings.HasPrefix(region, "cn-") {
			return "https://s3." + region + ".amazonaws.com.cn"
		}
		return "https://s3." + region + ".amazonaws.com"
	case "gcp":
		return "https://storage.googleapis.com"
	case "aliyun":
		if region == "" {
			return ""
		}
		return "https://oss-" + region + ".aliyuncs.com"
	case "tencent":
		if region == "" {
			return ""
		}
		return "https://cos." + region + ".myqcloud.com"
	case "huawei":
		if region == "" {
			return ""
		}
		return "https://obs." + region + ".myhuaweicloud.com"
	case "azure":
		r := strings.ToLower(region)
		if r == "" {
			return ""
		}
		if strings.HasPrefix(r, "china") {
			return "core.chinacloudapi.cn"
		}
		if strings.HasPrefix(r, "usgov") || strings.HasPrefix(r, "usdod") {
			return "core.usgovcloudapi.net"
		}
		if strings.HasPrefix(r, "germany") {
			return "core.cloudapi.de"
		}
		return "core.windows.net"
	}
	return ""
}

// InferCloudProviderFromScheme infers the cloud provider from the URI scheme.
func InferCloudProviderFromScheme(scheme string) string {
	switch strings.ToLower(strings.TrimSpace(scheme)) {
	case "oss":
		return oss.CloudProviderAliyun
	case "s3", "aws", "minio":
		return oss.CloudProviderAWS
	case "gs", "gcs":
		return oss.CloudProviderGCP
	case "cos":
		return oss.CloudProviderTencent
	case "obs":
		return oss.CloudProviderHuawei
	default:
		return ""
	}
}

// NewResolvedExternalObjectStore builds a ResolvedObjectStore from an external
// source URI and parsed spec. Credentials (role_arn/external_id/AK/SK/...)
// come from the collection spec, NOT from milvus minio configuration.
func NewResolvedExternalObjectStore(
	ctx context.Context,
	source string,
	spec ExternalSourceSpec,
	skipBucketCheck bool,
) (*oss.ResolvedObjectStore, ExternalSourceLocation, error) {
	location, err := ResolveExternalSource(source, spec)
	if err != nil {
		return nil, ExternalSourceLocation{}, err
	}

	provider := spec.CloudProvider
	if provider == "" {
		provider = InferCloudProviderFromScheme(location.Scheme)
	}
	if provider == "" {
		return nil, ExternalSourceLocation{}, fmt.Errorf("unsupported external source scheme/provider: %s/%s", location.Scheme, spec.CloudProvider)
	}

	useSSL := true
	if spec.UseSSL != nil {
		useSSL = *spec.UseSSL
	}

	bucket := location.Bucket
	if spec.BucketName != "" {
		bucket = spec.BucketName
	}

	addr := location.Address
	if addr == "" {
		addr = location.Host
	}

	param := oss.MinioClientParam{
		Addr:               addr,
		UseSSL:             useSSL,
		CloudProvider:      provider,
		Region:             spec.Region,
		AK:                 spec.AK,
		SK:                 spec.SK,
		UseIAM:             spec.UseIAM,
		IAMEndpoint:        spec.IAMEndpoint,
		RoleARN:            spec.RoleARN,
		RoleSessionName:    spec.RoleSessionName,
		ExternalID:         spec.ExternalID,
		LoadFrequency:      spec.LoadFrequency,
		AliyunRoleAuthMode: spec.AliyunRoleAuthMode,
		BucketName:         bucket,
		RootPath:           location.RootPath,
	}
	if provider == oss.CloudProviderAliyun && param.RoleARN != "" && param.AliyunRoleAuthMode == "" {
		param.AliyunRoleAuthMode = "oidc"
	}
	// Fallback to IAM chain when no explicit role arn / static key is present.
	if param.RoleARN == "" && param.AK == "" && param.SK == "" && !spec.Anonymous {
		param.UseIAM = true
	}
	oss.WithSkipCheckBucket(skipBucketCheck)(&param)

	client, err := oss.NewMinioClient(ctx, param)
	if err != nil {
		return nil, ExternalSourceLocation{}, err
	}
	return &oss.ResolvedObjectStore{
		Store:      oss.NewMinioObjectStore(client),
		BucketName: client.BucketName,
		RootPath:   client.RootPath,
	}, location, nil
}

// NewResolvedExternalObjectStoreFromSchema builds an external store from a raw
// external_source URI + external_spec JSON pair.
func NewResolvedExternalObjectStoreFromSchema(
	ctx context.Context,
	externalSource, externalSpec string,
	skipBucketCheck bool,
) (*oss.ResolvedObjectStore, ExternalSourceLocation, error) {
	if externalSource == "" {
		return nil, ExternalSourceLocation{}, fmt.Errorf("external source is empty")
	}
	spec, err := ParseExternalSpec(externalSpec)
	if err != nil {
		return nil, ExternalSourceLocation{}, err
	}
	return NewResolvedExternalObjectStore(ctx, externalSource, spec, skipBucketCheck)
}

// NewResolvedExternalObjectStoreFromCollection builds an external store from a
// collection model. Returns an error if the collection is not external.
func NewResolvedExternalObjectStoreFromCollection(
	ctx context.Context,
	collection *models.Collection,
	skipBucketCheck bool,
) (*oss.ResolvedObjectStore, ExternalSourceLocation, error) {
	proto := collection.GetProto()
	if proto.GetSchema().GetExternalSource() == "" {
		return nil, ExternalSourceLocation{}, fmt.Errorf("collection %d does not have external source", proto.GetID())
	}
	return NewResolvedExternalObjectStoreFromSchema(
		ctx,
		proto.GetSchema().GetExternalSource(),
		proto.GetSchema().GetExternalSpec(),
		skipBucketCheck,
	)
}

// ResolveExternalLocationFromCollection parses the collection's external source
// and spec into a location WITHOUT connecting to the external bucket or
// validating the file format. Commands that only annotate paths (e.g. show
// manifest) should use this instead of NewResolvedExternalObjectStoreFromCollection
// so they do not fail when external credentials or connectivity are broken.
func ResolveExternalLocationFromCollection(collection *models.Collection) (ExternalSourceLocation, error) {
	proto := collection.GetProto()
	schema := proto.GetSchema()
	if schema.GetExternalSource() == "" {
		return ExternalSourceLocation{}, fmt.Errorf("collection %d does not have external source", proto.GetID())
	}
	spec, err := ParseExternalSpecLoose(schema.GetExternalSpec())
	if err != nil {
		return ExternalSourceLocation{}, err
	}
	return ResolveExternalSource(schema.GetExternalSource(), spec)
}

// FileBackend indicates which object store a manifest file belongs to.
type FileBackend int

const (
	// FileBackendInternal means the file lives in milvus's own object storage.
	FileBackendInternal FileBackend = iota
	// FileBackendExternal means the file lives in the external collection's
	// source bucket and requires the collection-attached credentials.
	FileBackendExternal
)

func (b FileBackend) String() string {
	switch b {
	case FileBackendInternal:
		return "internal"
	case FileBackendExternal:
		return "external"
	default:
		return "unknown"
	}
}

// ManifestPathResolver routes a manifest file path to the correct object store.
//
// The discriminator matches milvus-storage's FilesystemCache::resolve_config:
//   - absolute URI (contains "://")  -> external store
//   - contains "ROOT_PATH"           -> external store (placeholder replaced)
//   - relative path (no scheme)      -> internal store, resolved under the
//     segment base path + dir prefix
//
// This is what makes a single manifest mixing external data files and internal
// function-output files (e.g. sparse vectors generated from varchar) work.
type ManifestPathResolver struct {
	internalStore    oss.ObjectStore
	externalStore    oss.ObjectStore
	manifestBasePath string
	location         ExternalSourceLocation
}

// NewManifestPathResolver builds a resolver. manifestBasePath is the resolved
// segment base path (e.g. {rootPath}/files/insert_log/{coll}/{part}/{seg}).
func NewManifestPathResolver(
	internalStore oss.ObjectStore,
	manifestBasePath string,
	externalStore oss.ObjectStore,
	location ExternalSourceLocation,
) *ManifestPathResolver {
	return &ManifestPathResolver{
		internalStore:    internalStore,
		externalStore:    externalStore,
		manifestBasePath: manifestBasePath,
		location:         location,
	}
}

// Resolve returns the store, resolved object key and backend for a manifest
// file path. dirPrefix is the layout directory of the section the path belongs
// to (_data, _delta, _stats, _index or ../lobs).
func (r *ManifestPathResolver) Resolve(filePath, dirPrefix string) (oss.ObjectStore, string, FileBackend, error) {
	trimmed := strings.TrimSpace(filePath)
	if trimmed == "" {
		return nil, "", 0, fmt.Errorf("manifest file path is empty")
	}

	if strings.Contains(trimmed, "://") {
		key, err := ResolveExternalObjectKey(r.location, trimmed)
		if err != nil {
			return nil, "", 0, err
		}
		return r.externalStore, key, FileBackendExternal, nil
	}

	if strings.Contains(trimmed, "ROOT_PATH") {
		key := strings.ReplaceAll(trimmed, "ROOT_PATH", r.location.RootPath)
		return r.externalStore, path.Clean(strings.TrimPrefix(key, "/")), FileBackendExternal, nil
	}

	rel := strings.TrimPrefix(strings.TrimSpace(trimmed), "/")
	key := path.Join(r.manifestBasePath, dirPrefix, rel)
	return r.internalStore, key, FileBackendInternal, nil
}

// ResolveExternalObjectKey resolves an external file reference (absolute URI,
// ROOT_PATH placeholder, or path relative to the external source) into an
// object key used with the external store. Absolute URIs are validated against
// the location according to its form (Milvus-form: host = endpoint, first path
// segment = bucket; AWS-form: host = bucket, full path = key).
func ResolveExternalObjectKey(location ExternalSourceLocation, externalFile string) (string, error) {
	trimmed := strings.TrimSpace(externalFile)
	if trimmed == "" {
		return "", fmt.Errorf("external file path is empty")
	}
	if strings.Contains(trimmed, "ROOT_PATH") {
		return strings.ReplaceAll(trimmed, "ROOT_PATH", location.RootPath), nil
	}
	if strings.Contains(trimmed, "://") {
		u, err := url.Parse(trimmed)
		if err != nil {
			return "", fmt.Errorf("parse external file path: %w", err)
		}
		if location.Form == LocationFormAWS {
			// scheme://bucket/key — host is the bucket, path is the key.
			if u.Host != "" && u.Host != location.Bucket {
				return "", fmt.Errorf("external file host %s does not match external source bucket %s", u.Host, location.Bucket)
			}
			trimmed = strings.TrimPrefix(u.Path, "/")
		} else {
			// scheme://endpoint/bucket/key — host is the endpoint.
			if u.Host != "" && u.Host != location.Address && u.Host != location.Host {
				return "", fmt.Errorf("external file host %s does not match external source endpoint %s", u.Host, location.Address)
			}
			parts := strings.Split(strings.Trim(u.Path, "/"), "/")
			if len(parts) == 0 || parts[0] == "" {
				return "", fmt.Errorf("external file path %s does not include bucket", trimmed)
			}
			if parts[0] != location.Bucket {
				return "", fmt.Errorf("external file bucket %s does not match external source bucket %s", parts[0], location.Bucket)
			}
			trimmed = strings.Join(parts[1:], "/")
		}
	}
	trimmed = strings.TrimPrefix(trimmed, "/")
	if location.RootPath == "" {
		return path.Clean(trimmed), nil
	}
	if trimmed == location.RootPath || strings.HasPrefix(trimmed, location.RootPath+"/") {
		return path.Clean(trimmed), nil
	}
	return path.Join(location.RootPath, trimmed), nil
}

func ReadMapString(payload map[string]any, key string) string {
	value, ok := payload[key]
	if !ok || value == nil {
		return ""
	}
	switch v := value.(type) {
	case string:
		return strings.TrimSpace(v)
	default:
		return strings.TrimSpace(fmt.Sprint(v))
	}
}

func ReadMapBool(payload map[string]any, key string) (bool, bool) {
	value, ok := payload[key]
	if !ok || value == nil {
		return false, false
	}
	switch v := value.(type) {
	case bool:
		return v, true
	case string:
		parsed, err := strconv.ParseBool(strings.TrimSpace(v))
		if err != nil {
			return false, false
		}
		return parsed, true
	default:
		return false, false
	}
}
