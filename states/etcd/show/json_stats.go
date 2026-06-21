package show

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/apache/arrow/go/v17/parquet/file"
	"github.com/apache/arrow/go/v17/parquet/pqarrow"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/oss"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/birdwatcher/states/ossutil"
)

// JSONStatsParam defines parameters for `show json-stats` command.
type JSONStatsParam struct {
	framework.ParamBase `use:"show json-stats" desc:"display json key stats from datacoord meta"`
	CollectionID        int64  `name:"collection" default:"0" desc:"collection id to filter with"`
	PartitionID         int64  `name:"partition" default:"0" desc:"partition id to filter with"`
	SegmentID           int64  `name:"segment" default:"0" desc:"segment id to filter with"`
	FieldID             int64  `name:"field" default:"0" desc:"field id to filter with"`
	State               string `name:"state" default:"Flushed" desc:"target segment state"`
	Detail              bool   `name:"detail" default:"false" desc:"print details including files & memory size"`
	ShowSchema          bool   `name:"show-schema" default:"false" desc:"print schema"`
	KeyPrefix           string `name:"prefix" default:"" desc:"filter json keys by prefix when parsing layout map"`
	PrintShared         bool   `name:"print-shared" default:"true" desc:"print shared keys data layout"`
}

// JSONStatsCommand implements `show json-stats` using etcd meta (same source as show segment).
func (c *ComponentShow) JSONStatsCommand(ctx context.Context, p *JSONStatsParam) error {
	segments, err := common.ListSegments(ctx, c.client, c.metaPath, func(segment *models.Segment) bool {
		return (p.CollectionID == 0 || segment.CollectionID == p.CollectionID) &&
			(p.PartitionID == 0 || segment.PartitionID == p.PartitionID) &&
			(p.SegmentID == 0 || segment.ID == p.SegmentID) &&
			(p.State == "" || strings.EqualFold(segment.State.String(), p.State))
	})
	if err != nil {
		fmt.Println("failed to list segments", err.Error())
		return nil
	}

	if len(segments) == 0 {
		fmt.Println("no segments found")
		return nil
	}

	total := 0
	built := 0
	var notBuiltSegIDs []int64

	var (
		store    oss.ObjectStore
		rootPath string
		ossReady bool
	)

	for _, seg := range segments {
		total++

		builtForSegment := false
		if p.FieldID != 0 {
			if seg.JsonKeyStats != nil {
				if _, ok := seg.JsonKeyStats[p.FieldID]; ok {
					builtForSegment = true
				}
			}
		} else if len(seg.JsonKeyStats) > 0 {
			builtForSegment = true
		}

		if !builtForSegment {
			notBuiltSegIDs = append(notBuiltSegIDs, seg.ID)
			continue
		}

		built++
		printedHeader := false
		for fieldID, keyStats := range seg.JsonKeyStats {
			if p.FieldID != 0 && p.FieldID != fieldID {
				continue
			}
			if !printedHeader {
				fmt.Printf("Segment %d (Collection %d, Partition %d) JsonKeyStats:\n", seg.ID, seg.CollectionID, seg.PartitionID)
				printedHeader = true
			}

			fmt.Printf("  field[%d]: version=%d files=%d mem=%d build=%d format=%d\n",
				fieldID,
				keyStats.GetVersion(),
				len(keyStats.GetFiles()),
				keyStats.GetMemorySize(),
				keyStats.GetBuildID(),
				keyStats.GetJsonKeyStatsDataFormat(),
			)
			if !p.Detail {
				continue
			}

			shreddingPath := fmt.Sprintf("ROOT_PATH/json_stats/%d/%d/%d/%d/%d/%d/%d/shredding_data/0/0",
				keyStats.GetJsonKeyStatsDataFormat(),
				keyStats.GetBuildID(),
				keyStats.GetVersion(),
				seg.CollectionID,
				seg.PartitionID,
				seg.ID,
				fieldID)
			if !ossReady {
				resolvedStore, err := ossutil.GetObjectStoreFromCfg(ctx, c.client, c.metaPath)
				if err != nil {
					fmt.Printf("      [s3] skip reading (init error): %s\n", err.Error())
				} else {
					store, rootPath = resolvedStore.Store, resolvedStore.RootPath
					ossReady = true
				}
			}
			if !ossReady {
				continue
			}

			objKey := oss.ResolveObjectKey(rootPath, shreddingPath)
			metaPath := fmt.Sprintf("ROOT_PATH/json_stats/%d/%d/%d/%d/%d/%d/%d/meta.json",
				keyStats.GetJsonKeyStatsDataFormat(),
				keyStats.GetBuildID(),
				keyStats.GetVersion(),
				seg.CollectionID,
				seg.PartitionID,
				seg.ID,
				fieldID)
			metaKey := oss.ResolveObjectKey(rootPath, metaPath)
			if !readJSONMeta(ctx, store, metaKey, p) {
				readParquetFooterSummary(ctx, store, objKey)
				readParquetMeta(ctx, store, objKey, p)
			}
		}
	}

	if total > 0 {
		percent := float64(built) * 100.0 / float64(total)
		fmt.Printf("\n--- JsonKeyStats built: %d/%d (%.2f%%)\n", built, total, percent)
		if len(notBuiltSegIDs) > 0 {
			fmt.Printf("--- Not built segments (%d): %v\n", len(notBuiltSegIDs), notBuiltSegIDs)
		}
	}

	return nil
}

// readParquetFooterSummary reads last 8 bytes of parquet to show metadata length and magic
func readParquetFooterSummary(ctx context.Context, store oss.ObjectStore, key string) {
	key, size, err := resolveParquetKey(ctx, store, key)
	if err != nil {
		fmt.Printf("      [parquet] stat failed: %v, key=%s\n", err, key)
		return
	}
	if size < 8 {
		fmt.Printf("      [parquet] file too small for footer, size=%d, key=%s\n", size, key)
		return
	}
	obj, err := store.Open(ctx, key, oss.WithOpenRange(size-8, size-1))
	if err != nil {
		fmt.Printf("      [parquet] read footer failed: %s, key=%s\n", err.Error(), key)
		return
	}
	if closer, ok := obj.(io.Closer); ok {
		defer closer.Close()
	}
	buf := make([]byte, 8)
	n, err := io.ReadFull(obj, buf)
	if n != 8 {
		fmt.Printf("      [parquet] read footer bytes failed: %v, n=%d, key=%s\n", err, n, key)
		return
	}
	magic := string(buf[4:8])
	metaLen := uint32(buf[0]) | uint32(buf[1])<<8 | uint32(buf[2])<<16 | uint32(buf[3])<<24
	fmt.Printf("      [parquet] footer: meta_len=%d magic=%s, key=%s\n", metaLen, magic, key)
}

// jsonStatsMeta represents the structure of meta.json file
type jsonStatsMeta struct {
	Version             string            `json:"version"`
	NumRows             int64             `json:"num_rows"`
	NumShreddingColumns int64             `json:"num_shredding_columns"`
	LayoutTypeMap       map[string]string `json:"layout_type_map"`
}

// readJSONMeta reads the meta.json file from S3 and prints layout information
func readJSONMeta(ctx context.Context, store oss.ObjectStore, metaKey string, p *JSONStatsParam) bool {
	obj, err := store.Open(ctx, metaKey)
	if err != nil {
		return false
	}
	if closer, ok := obj.(io.Closer); ok {
		defer closer.Close()
	}

	data, err := io.ReadAll(obj)
	if err != nil {
		return false
	}

	var meta jsonStatsMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		fmt.Printf("      [meta.json] failed to parse: %s\n", err.Error())
		return false
	}

	fmt.Printf("      [meta.json] version=%s num_rows=%d num_shredding_columns=%d\n",
		meta.Version, meta.NumRows, meta.NumShreddingColumns)

	if meta.LayoutTypeMap != nil {
		printLayoutTypeMap(meta.LayoutTypeMap, p)
	}

	return true
}

// printLayoutTypeMap prints the layout type map with shared/non-shared key separation
func printLayoutTypeMap(mm map[string]string, p *JSONStatsParam) {
	var nonSharedKeys []string
	sharedCount := 0
	for k, val := range mm {
		if p.KeyPrefix != "" && !strings.HasPrefix(k, p.KeyPrefix) {
			continue
		}
		if val == "SHARED" {
			sharedCount++
		} else {
			nonSharedKeys = append(nonSharedKeys, k)
		}
	}

	if p.PrintShared {
		fmt.Printf("        shared keys: ")
		first := true
		for k, val := range mm {
			if p.KeyPrefix != "" && !strings.HasPrefix(k, p.KeyPrefix) {
				continue
			}
			if val == "SHARED" {
				if !first {
					fmt.Printf(", ")
				}
				fmt.Printf("%s", k)
				first = false
			}
		}
		if sharedCount > 0 {
			fmt.Println()
		} else {
			fmt.Println("(none)")
		}
	}
	if len(nonSharedKeys) > 0 {
		fmt.Printf("        non-shared keys: %s\n", strings.Join(nonSharedKeys, ", "))
	}
	totalCount := sharedCount + len(nonSharedKeys)
	fmt.Printf("        layout summary: shared=%d items, non-shared=%d items, total=%d items\n",
		sharedCount, len(nonSharedKeys), totalCount)
}

func readParquetMeta(ctx context.Context, store oss.ObjectStore, key string, p *JSONStatsParam) {
	obj, err := store.Open(ctx, key)
	if err != nil {
		fmt.Printf("      [parquet] get object failed: %s, key=%s\n", err.Error(), key)
		return
	}
	if closer, ok := obj.(io.Closer); ok {
		defer closer.Close()
	}
	pqReader, err := file.NewParquetReader(obj)
	if err != nil {
		fmt.Printf("      [parquet] open Parquet reader failed: %s, key=%s\n", err.Error(), key)
		return
	}

	arrReader, err := pqarrow.NewFileReader(pqReader, pqarrow.ArrowReadProperties{BatchSize: 1024}, memory.DefaultAllocator)
	if err != nil {
		fmt.Printf("      [parquet] open pq\t file reader failed: %s, key=%s\n", err.Error(), key)
		return
	}

	if p.ShowSchema {
		schema, err := arrReader.Schema()
		if err != nil {
			fmt.Printf("      [parquet] get parquet schema failed: %s, key=%s\n", err.Error(), key)
			return
		}
		if se := schema.Fields(); len(se) > 0 {
			fmt.Printf("      [parquet] schema elements: %d\n", len(se))
			for i, col := range se {
				fmt.Printf("        [%d] name=%s type=%v\n", i, col.Name, col.Type)
			}
		}
	}

	kvMetaData := pqReader.MetaData().KeyValueMetadata()
	if kvMetaData != nil && kvMetaData.Len() > 0 {
		fmt.Printf("      [parquet] metadata entries: %d\n", kvMetaData.Len())
		for i := 0; i < kvMetaData.Len(); i++ {
			key := kvMetaData.Keys()[i]
			val := kvMetaData.Values()[i]
			if key == "key_layout_type_map" {
				continue
			}
			if key == "row_group_metadata" {
				parts := strings.Split(val, ";")
				cnt := 0
				for _, p := range parts {
					if strings.TrimSpace(p) != "" {
						cnt++
					}
				}
				fmt.Printf("        row_group_metadata groups: %d\n", cnt)
				continue
			}
			fmt.Printf("        [%d] %s: %s\n", i, key, val)
		}
	}

	v := kvMetaData.FindValue("key_layout_type_map")
	if v != nil {
		var mm map[string]string
		if err := json.Unmarshal([]byte(*v), &mm); err != nil {
			fmt.Printf("        kv: key_layout_type_map (invalid json): %v\n", err)
			return
		}
		printLayoutTypeMap(mm, p)
	}
}

func resolveParquetKey(ctx context.Context, store oss.ObjectStore, key string) (string, int64, error) {
	stat, err := store.Stat(ctx, key)
	if err == nil {
		return key, stat.Size, nil
	}

	prefix := key
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	ch, listErr := store.List(ctx, prefix, true)
	if listErr != nil {
		return key, 0, err
	}
	for objInfo := range ch {
		if objInfo.Err != nil {
			return key, 0, objInfo.Err
		}
		if objInfo.IsDir {
			continue
		}
		fmt.Printf("      [parquet] using first object under prefix: %s\n", objInfo.Key)
		return objInfo.Key, objInfo.Size, nil
	}
	return key, 0, err
}
