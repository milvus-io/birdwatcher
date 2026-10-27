# Binlog readers

`scan-binlog` supports these internal storage layouts:

- v1 (including legacy version 0): one field per Milvus-wrapped Parquet binlog.
- v2: packed Parquet column groups, enumerated through segment binlog metadata.
- v3: column groups and delete files enumerated from the segment's immutable
  manifest reference. Selected Parquet groups are streamed and joined by logical
  row position, across independent file, row-group and Arrow batch boundaries.

For v3, `count`, `count-distinct`, `locate`, `dedup`, `--fields` and `--expr` use the same row
filters and tasks as the older layouts. `locate` reports the manifest reference,
segment-global row offset, PK file and file-local offset. Fields added after a
segment was written are filled with their schema defaults or nulls.

Deletes are read from the manifest for v3 and segment metadata for older layouts.
Both legacy JSON/multifield binlogs and packed PK/timestamp Parquet delete files
are supported. Applicable L0 deletes are included when selecting a single
segment; `--ignoreDelete` skips all delete-file reads and filtering.

Internal v3 data files and manifest-referenced deltalogs use a shared `Read` /
`ReadAt` range buffer per open object, defaulting to **10 MiB (10485760 bytes)**.
Adjacent small Parquet reads, including footer and metadata probes, reuse an
aligned cached range. Reads at least as large as the buffer bypass it to avoid
splitting a large column-chunk request into many GETs. File size is cached and
`ReadAt` keeps the sequential seek position unchanged. Buffer allocation is lazy
and bounded by the smaller of the configured size and object size; total buffer
memory scales with concurrently open data/delta objects.

Use `--readBufferSize` to change the size in bytes; `0` also selects the default.
This is independent of the Arrow row batch size and does not change row ranges,
filtering, or counting. Under `--verbose`, the I/O lines describe actual buffer
fills or bypass reads; cache hits do not generate OSS I/O log lines. Legacy
v1/v2 scanning keeps its existing read path.

```text
scan-binlog --collection 123 --action count --readBufferSize 4194304 --verbose
```

Use `--action count-distinct --field <name>` to count a field's different
non-null values after delete and expression filtering, across all matched
segments. `--field` is required for this action and is independent of `--fields`:
the target is automatically added to the read fields, and `--fields` can supply
additional fields used by `--expr`. Missing target fields and unsupported types
fail before connecting to object storage. `--field` is rejected for other actions.

```text
scan-binlog --collection 123 --action count-distinct --field tenant_id --verbose
scan-binlog --collection 123 --action count-distinct --field tenant_id --fields status --expr 'status == "active"'
```

The result is exact and includes matched-row and skipped-null counts. All workers
share the same distinct set; `--outputLimit` does not limit the count. Supported
types are Bool, Int8/16/32/64, Float, Double, String and VarChar. Integer precision
is preserved, strings are case-sensitive, positive and negative zero compare
equal, and all NaN representations count as one value. Arrays, JSON, TEXT/LOB and
vectors are unsupported. Memory usage grows with the number and size of unique
values; no approximation or disk spill is performed.

Use `--verbose` to write detailed diagnostics to stderr. Logs include metadata
queries, worker/segment scopes, manifest and delete loading, column-group and
Arrow batch progress, and object `Read`/`ReadAt`/`Seek`/`Close` calls. Each operation
has a start/result pair, an ID and elapsed time. Operations still pending after
ten seconds produce repeated `waiting` lines, including when an object handle
opens lazily and its first actual read stalls. Logging is off by default.

```text
scan-binlog --collection 123 --action count --verbose
```

The v3 reader supports Parquet and existing Go field deserializers. Selecting a
column group stored in Vortex returns an explicit unsupported-format error.
TEXT/LOB decoding is not supported. `detect-vector-nulls` retains its separate
external-collection scan path. The default build needs no native storage library.
