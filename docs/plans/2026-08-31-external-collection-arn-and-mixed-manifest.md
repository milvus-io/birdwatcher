# External Collection Object Store Credential Passing and Mixed Manifest Support

**Goal:** Fix birdwatcher's `show manifest` / `scan-binlog` access to external collection data files. The ARN information required by external collections (`role_arn`, `external_id`, `session_name`, AK/SK, etc.) does **not** come from Milvus component configuration; it is attached to the collection schema (`external_source` URI + the `extfs` field of the `external_spec` JSON). This design unifies how these credentials are passed through and supports the mixed scenario where a single manifest contains both external data files and internal function-output files (e.g. sparse vectors generated from a varchar column).

**Architecture:** Add a shared layer `states/ossutil/external.go` encapsulating external spec/source parsing, external object store client construction, and a **per-file** `ManifestPathResolver`. The three commands `show manifest`, `scan-binlog`, and `inspect-parquet --external` reuse this layer: they read the ARN credentials from the collection to build the external store, then route each file in the manifest to the internal or external store through the resolver.

**Tech stack:** Go, minio-go/v7, Arrow/Parquet, Birdwatcher states/ossutil, Milvus `schemapb` / `etcdpb`.

---

## Background and Problem

### Credential source mismatch

- `show manifest` (`states/show_manifest.go`) and `scan-binlog` (`states/scan_binlog.go`) originally both created their object store client via `s.GetObjectStore(...)` (`states/instance.go:151`), which ultimately goes through `ossutil.GetObjectStoreFromCfg` (`states/ossutil/minio_cfg.go:116`). That pulls component configuration over gRPC and reads keys such as `minio.rolearn` / `minio.externalid` from **Milvus's own minio configuration**.
- However, external collection data files live in the user's external bucket, and the credentials to access them (`role_arn`, `external_id`, `session_name`, AK/SK, `use_iam`, etc.) **exist only in the collection schema**:
  - `schema.ExternalSource`: the external source URI, e.g. `oss://oss-cn-hangzhou.aliyuncs.com/bucket/root/...`
  - `schema.ExternalSpec`: a JSON string, e.g. `{"format":"parquet","extfs":{"role_arn":"...","external_id":"...",...}}`
- Consequently the original implementation cannot access the real data files of an external collection with the correct credentials.

### Mixed manifest scenario

External collections may declare functions (BM25 / embedding). Their output columns (e.g. sparse vectors) are computed by DataNode `ExecuteFunctionsForSegment` (milvus `internal/datanode/external/function_executor.go`) and written to **Milvus internal storage** (`{rootPath}/files/insert_log/.../_data/...`), then appended to the same manifest as a new column group. The result is a single manifest that contains both:
- **External files**: absolute URIs (e.g. `s3://bucket/data.parquet`, `oss://endpoint/bucket/...`), requiring external bucket credentials;
- **Internal files**: relative paths (e.g. `abc.parquet`, `bm25.3/0`), requiring Milvus configuration credentials.

A design that routes "everything through the external store" will necessarily fail on mixed manifests.

---

## Key Facts (verified against milvus / milvus-storage code)

1. **The manifest file itself always lives in Milvus internal storage.**
   `createManifestForSegment` (milvus `internal/datanode/external/task_update.go:1062`) writes `{rootPath}/files/insert_log/{coll}/{part}/{seg}/_metadata/manifest-{ver}.avro`; `MarshalManifestPath` (`internal/storagev2/packed/ffi_common.go:355`) stores that base path into the `base_path` field of the segment's `ManifestPath` JSON.

2. **Path storage/restore rules inside the manifest** (milvus-storage `cpp/src/manifest.cpp`)
   - On write, `toRelativePaths(base_path)`: internal files (prefixed by `{base_path}/_data|_delta|_stats|_index|../lobs`) are trimmed to relative paths; external files keep their absolute URI as-is.
   - On read, `ToAbsolutePaths(base_path)`: relative paths without a scheme are re-joined to `{base_path}/<dir>`; absolute URIs with a scheme are returned unchanged.

3. **milvus-storage routes the storage backend per file** (`cpp/src/filesystem/fs.cpp`, `FilesystemCache::resolve_config`)
   - Absolute URI (has a scheme) → matched against `extfs.*` properties by address+bucket → external FS (carrying ARN/external_id etc.);
   - Path without a scheme (relative) → default `fs.*` → internal Milvus storage.
   - In other words: **mixed manifests are natively supported; the discriminator is "is it an absolute URI"**.

4. **Function output columns are written to internal storage.**
   `ExecuteFunctionsForSegment` writes output packed files to `{rootPath}/files/insert_log/.../_data/...`, and BM25 stats to `{base_path}/_stats/bm25.{id}/0` (`function_executor.go:454`).

---

## Design

### Core discriminator
> **Path contains a scheme (`://`) → external; path is relative (no scheme) → internal.**

This matches milvus-storage's native discriminator and does not depend on schema field mapping.

### Part 1: Shared layer — `states/ossutil/external.go` (new)

**Public API:**

| Symbol | Description |
| --- | --- |
| `ExternalSourceSpec` | Parsed external spec; `extfs` keys aligned with milvus `specutil.ExtfsKey*`: `cloud_provider/region/role_arn/session_name/external_id/access_key_id/access_key_value/use_iam/iam_endpoint/bucket_name/storage_type/use_ssl/load_frequency/anonymous/aliyun_role_auth_mode` |
| `ParseExternalSpec(raw)` | Parses the `schema.ExternalSpec` JSON; only the `parquet` format is supported |
| `ExternalSourceLocation` | Parsed external source URI: `Scheme/Host/Bucket/RootPath` |
| `ParseExternalSource(raw)` | Parses the `schema.ExternalSource` URI |
| `InferCloudProviderFromScheme(scheme)` | Infers `oss/aws/gcp/tencent/huawei` |
| `NewResolvedExternalObjectStore(ctx, source, spec, skipBucketCheck)` | Builds `*oss.ResolvedObjectStore` + location from external credentials; `RoleARN`/`ExternalID` etc. come from the collection, not configuration |
| `NewResolvedExternalObjectStoreFromSchema(ctx, externalSource, externalSpec, skipBucketCheck)` | Convenience wrapper |
| `NewResolvedExternalObjectStoreFromCollection(ctx, collection, skipBucketCheck)` | Builds from a collection model; errors if the collection is not external |
| `FileBackend` | Enum `FileBackendInternal` / `FileBackendExternal` |
| `ManifestPathResolver` | Per-file routing: `Resolve(filePath, dirPrefix) (store, key, backend, err)` |
| `ResolveExternalObjectKey(location, externalFile)` | External file reference (absolute URI / `ROOT_PATH` / relative path) → object key |

**`ManifestPathResolver.Resolve` routing logic:**
- Contains `://` → `ResolveExternalObjectKey` → external store;
- Contains `ROOT_PATH` → replaced with `location.RootPath` → external store;
- Otherwise, relative path → `path.Join(manifestBasePath, dirPrefix, rel)` → internal store.
- `dirPrefix` is taken per section as `_data` / `_delta` / `_stats` / `_index` / `../lobs`.

### Part 2: `show manifest` (`states/show_manifest.go`)

1. Resolve the collection: from `--collection`, or by looking up `CollectionID` from the first manifest segment.
2. If `schema.GetExternalSource() != ""`: build the external store via `NewResolvedExternalObjectStoreFromCollection`; the manifest file itself is still read from the internal store (`s.GetObjectStore`).
3. When printing, annotate every column group / delta / stats / index / lob file with `Backend: internal|external` and the final object key via `ManifestPathResolver.Resolve` (`printManifestWithResolver`).
4. JSON output mode and non-external collection behavior remain unchanged.

### Part 3: `scan-binlog` (`states/scan_binlog.go` + new `states/scan_binlog_external.go`)

1. Detect an external collection (`isExternalCollection`); if so, build the external store + location.
2. In `getObject`, for delta logs: when the path is external (`isExternalPath`: contains `://` or `ROOT_PATH`), route through `ResolveExternalObjectKey` to the external store; otherwise use the internal store.
3. In `workFn`, external segments that have a manifest go through `scanExternalSegment`:
   - Read the manifest from the internal store;
   - Iterate column groups, selecting the store per file via the resolver;
   - Read parquet (Arrow), mapping manifest column names to field IDs (`external_field`, or the field ID string / field name);
   - Deserialize cells, set the PK, apply the filters (`loEntryFilter`/`deltalogFilter`/`exprFilter`), and feed the `scanTask`.
   - Segments that are not external / have no manifest keep the original binlog iteration path.

### Part 4: `inspect-parquet --external` (`states/inspect_parquet.go`)

- Remove the local duplicated external parsing / client-construction implementations and delegate to `ossutil` (`parseExternalSpec`/`parseExternalSource`/`newExternalMinioClient`/`resolveExternalObjectKey` become thin wrappers or aliases, keeping existing call sites and tests unchanged).
- `inspectExternalManifestParquet` switches to `ManifestPathResolver`: external column groups follow the original logic; internal column groups (function outputs) are read from the internal store, supporting per-file display of mixed column groups.

---

## File List

**New files:**
- `states/ossutil/external.go`
- `states/ossutil/external_test.go`
- `states/scan_binlog_external.go`

**Modified files:**
- `states/show_manifest.go`
- `states/scan_binlog.go`
- `states/inspect_parquet.go`

---

## Testing Strategy

- `states/ossutil/external_test.go`:
  - `ParseExternalSpec` with all `extfs` keys, empty spec, and rejection of an unsupported format;
  - `ParseExternalSource` URI parsing;
  - `ResolveExternalObjectKey` for relative path / already-rooted / full URI;
  - `InferCloudProviderFromScheme`;
  - **`ManifestPathResolver` mixed manifest**: external absolute URI, external `ROOT_PATH`, internal function-output file (`_data`), internal bm25 stats (`_stats`).
- The existing external parsing tests in the `states` package's `inspect_parquet_test.go` remain green (preserved via thin wrappers).

## Verification Commands

```bash
go build ./...
go test ./states/ossutil/ ./states/
gofmt -l states/ossutil/external.go states/ossutil/external_test.go states/scan_binlog_external.go states/show_manifest.go states/scan_binlog.go states/inspect_parquet.go
go vet ./states/ ./states/ossutil/
```

Note: the repository `.golangci.yml` is in v2 format and cannot be parsed by the golangci-lint binary currently available in this environment (a tool-version mismatch, not a code issue); lint is therefore validated via `go vet` + `gofmt` plus a temporary v1 config. In `go test ./...`, the build failures in `states/etcd/show` and `states/milvusctl` are caused by untracked test files referencing nonexistent fields and are unrelated to this change.

---

## Risks and Compatibility

1. **Non-external collection behavior is unchanged:** the resolver is only enabled when `ExternalSource != ""`; all other paths fully preserve the original logic.
2. **milvus-table format:** its column group files may reference internal source manifests (property `milvusTableSourceManifestPathProperty`). The current resolver covers most cases via "no scheme = internal"; recognizing the milvus-table-specific property is left for follow-up.
3. **`LoadFrequency` / auth mode:** when `use_iam`, `anonymous`, and AK/SK are all absent in the external spec, the resolver falls back to the IAM chain; Aliyun `role_arn` defaults to `oidc` (consistent with the existing inspect-parquet behavior).
4. **LOB files:** the directory prefix is `../lobs`; the relative path up-traversal is normalized by `path.Join`.
5. **Real cloud dependencies:** unit tests only cover parsing and routing and never issue real object store requests; real connectivity is left to manual verification.
