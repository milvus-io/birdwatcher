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
	"github.com/minio/minio-go/v7"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
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
		mclient    *minio.Client
		bucketName string
		rootPath   string
		ossReady   bool
	)

	for _, seg := range segments {
		// only segments matching the high-level filters are considered
		total++

		// determine built status according to field filter
		builtForSegment := false
		if p.FieldID != 0 {
			if seg.JsonKeyStats != nil {
				if _, ok := seg.JsonKeyStats[p.FieldID]; ok {
					builtForSegment = true
				}
			}
		} else {
			if len(seg.JsonKeyStats) > 0 {
				builtForSegment = true
			}
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
			if p.Detail {
				fmt.Printf("  field[%d]: version=%d files=%d mem=%d build=%d format=%d\n",
					fieldID,
					keyStats.GetVersion(),
					len(keyStats.GetFiles()),
					keyStats.GetMemorySize(),
					keyStats.GetBuildID(),
					keyStats.GetJsonKeyStatsDataFormat(),
				)
				shreddingPath := fmt.Sprintf("ROOT_PATH/json_stats/%d/%d/%d/%d/%d/%d/%d/shredding_data/0/0",
					keyStats.GetJsonKeyStatsDataFormat(),
					keyStats.GetBuildID(),
					keyStats.GetVersion(),
					seg.CollectionID,
					seg.PartitionID,
					seg.ID,
					fieldID)
				if !ossReady {
					cli, bucket, root, err := ossutil.GetMinioClientFromCfg(ctx, c.client, c.metaPath)
					if err != nil {
						fmt.Printf("      [s3] skip reading (init error): %s\n", err.Error())
					} else {
						mclient, bucketName, rootPath = cli, bucket, root
						ossReady = true
					}
				}
				if ossReady {
					objKey := strings.ReplaceAll(shreddingPath, "ROOT_PATH", rootPath)
					// fmt.Printf("      [s3] objKey: %s\n", objKey)
					// parquet footer summary
					readParquetFooterSummary(ctx, mclient, bucketName, objKey)
					// full metadata
					readParquetMeta(ctx, mclient, bucketName, objKey, p)
				}
			} else {
				fmt.Printf("  field[%d]: version=%d files=%d mem=%d build=%d format=%d\n",
					fieldID,
					keyStats.GetVersion(),
					len(keyStats.GetFiles()),
					keyStats.GetMemorySize(),
					keyStats.GetBuildID(),
					keyStats.GetJsonKeyStatsDataFormat(),
				)
			}
		}
	}

	// summary
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
func readParquetFooterSummary(ctx context.Context, client *minio.Client, bucket, key string) {
	// Try stat on the exact key first
	stat, err := client.StatObject(ctx, bucket, key, minio.StatObjectOptions{})
	if err != nil || stat.Size < 8 {
		// Treat as prefix, list first object under it
		prefix := key
		if !strings.HasSuffix(prefix, "/") {
			prefix += "/"
		}
		ch := client.ListObjects(ctx, bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true})
		objInfo, ok := <-ch
		if !ok || objInfo.Err != nil {
			fmt.Printf("      [parquet] stat failed: %v, key=%s\n", err, key)
			return
		}
		key = objInfo.Key
		stat.Size = objInfo.Size
		fmt.Printf("      [parquet] using first object under prefix: %s\n", key)
		if stat.Size < 8 {
			fmt.Printf("      [parquet] file too small for footer, size=%d, key=%s\n", stat.Size, key)
			return
		}
	}
	// read last 8 bytes: <metadata_len uint32 little-endian><'PAR1'>
	opts := minio.GetObjectOptions{}
	opts.SetRange(stat.Size-8, stat.Size-1)
	obj, err := client.GetObject(ctx, bucket, key, opts)
	if err != nil {
		fmt.Printf("      [parquet] read footer failed: %s, key=%s\n", err.Error(), key)
		return
	}
	defer obj.Close()
	buf := make([]byte, 8)
	n, err := io.ReadFull(obj, buf)
	if n != 8 {
		fmt.Printf("      [parquet] read footer bytes failed: %v, n=%d, key=%s\n", err, n, key)
		return
	}
	// io.ReadFull returns EOF when exactly filled; accept it
	magic := string(buf[4:8])
	metaLen := uint32(buf[0]) | uint32(buf[1])<<8 | uint32(buf[2])<<16 | uint32(buf[3])<<24
	fmt.Printf("      [parquet] footer: meta_len=%d magic=%s, key=%s\n", metaLen, magic, key)
}

func readParquetMeta(ctx context.Context, client *minio.Client, bucket, key string, p *JSONStatsParam) {
	obj, err := client.GetObject(ctx, bucket, key, minio.GetObjectOptions{})
	if err != nil {
		fmt.Printf("      [parquet] get object failed: %s, key=%s\n", err.Error(), key)
		return
	}
	pqReader, err := file.NewParquetReader(obj)
	if err != nil {
		fmt.Printf("      [parquet] open Parquet reader failed: %s, key=%s\n", err.Error(), key)
		return
	}

	arrReader, err := pqarrow.NewFileReader(pqReader, pqarrow.ArrowReadProperties{BatchSize: 1024}, memory.DefaultAllocator)
	if err != nil {
		fmt.Printf("      [parquet] open pq	 file reader failed: %s, key=%s\n", err.Error(), key)
		return
	}

	// Print schema elements
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
	v := kvMetaData.FindValue("key_layout_type_map")
	if v != nil {
		var mm map[string]string
		if err := json.Unmarshal([]byte(*v), &mm); err != nil {
			fmt.Printf("        kv: key_layout_type_map (invalid json): %v\n", err)
			return
		}

		sharedCount := 0
		nonCount := 0
		// calculate and print layout summary at the end
		for k, val := range mm {
			if p.KeyPrefix != "" && !strings.HasPrefix(k, p.KeyPrefix) {
				continue
			}
			fmt.Printf("        layout: %s=%s\n", k, val)

			if val == "SHARED" {
				sharedCount++
			} else {
				nonCount++
			}
		}
		totalCount := sharedCount + nonCount
		fmt.Printf("        layout summary: shared=%d items, non-shared=%d items, total=%d items\n",
			sharedCount, nonCount, totalCount)
	}
}
