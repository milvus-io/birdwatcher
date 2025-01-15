package show

import (
	"context"
	"fmt"
	"sort"
	"strings"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/birdwatcher/framework"
)

type EtcdKVTree struct {
	framework.ParamBase `use:"show etcd-kv-tree" desc:"show etcd kv tree with key size of each prefix"`
	Prefix              string `name:"prefix" default:"" desc:"the kv prefix to show"`
	Level               int64  `name:"level" default:"1" desc:"the level of kv tree to show"`
	TopK                int64  `name:"topK" default:"10" desc:"the number of top prefixes to show per level"`
}

// EtcdKVTreeCommand retrieves and prints the top K prefixes and their key counts up to the specified level
func (c *ComponentShow) EtcdKVTreeCommand(ctx context.Context, p *EtcdKVTree) error {
	// Fetch all keys under the given prefix
	resp, err := c.client.Get(ctx, p.Prefix, clientv3.WithPrefix(), clientv3.WithKeysOnly())
	if err != nil {
		return err
	}

	// Extract keys from the response
	keys := make([]string, len(resp.Kvs))
	for i, kv := range resp.Kvs {
		keys[i] = string(kv.Key)
	}

	// Count keys for prefixes up to the specified level
	result := countKeysAtEachLevel(keys, p.Prefix, int(p.Level))

	// Print the result with topK prefixes for each level in order
	printLevelsInOrder(result, int(p.TopK))

	return nil
}

// countKeysAtEachLevel counts the keys for each prefix at each level up to the specified level
func countKeysAtEachLevel(keys []string, basePrefix string, maxLevel int) map[int]map[string]int {
	levelStats := make(map[int]map[string]int)

	for _, key := range keys {
		// Ensure the key is under the base prefix
		if !strings.HasPrefix(key, basePrefix) {
			continue
		}

		// Process prefixes for each level up to maxLevel
		for level := 1; level <= maxLevel; level++ {
			prefix := getNthLevelPrefix(key, basePrefix, level)
			if prefix == "" {
				break
			}

			if _, exists := levelStats[level]; !exists {
				levelStats[level] = make(map[string]int)
			}
			levelStats[level][prefix]++
		}
	}

	return levelStats
}

// printLevelsInOrder ensures that levels are printed in order from 1 to maxLevel
func printLevelsInOrder(result map[int]map[string]int, topK int) {
	// Iterate over the levels in sorted order (from 1 to maxLevel)
	for level := 1; level <= len(result); level++ {
		if prefixes, exists := result[level]; exists {
			printTopKPrefixes(level, prefixes, topK)
		}
	}
}

// printTopKPrefixes prints the top K prefixes for a given level
func printTopKPrefixes(level int, prefixes map[string]int, topK int) {
	// Convert map to slice for sorting
	type prefixCount struct {
		prefix string
		count  int
	}
	var sortedPrefixes []prefixCount
	for prefix, count := range prefixes {
		sortedPrefixes = append(sortedPrefixes, prefixCount{prefix, count})
	}

	// Sort the prefixes by count in descending order
	sort.Slice(sortedPrefixes, func(i, j int) bool {
		return sortedPrefixes[i].count > sortedPrefixes[j].count
	})

	// Keep only the top K prefixes
	if len(sortedPrefixes) > topK {
		sortedPrefixes = sortedPrefixes[:topK]
	}

	// Print the result
	fmt.Printf("Level %d:\n", level)
	for _, p := range sortedPrefixes {
		fmt.Printf("  Prefix: %s, Key Count: %d\n", strings.TrimPrefix(p.prefix, "/"), p.count)
	}
}

// getNthLevelPrefix extracts the prefix up to the specified level under basePrefix
func getNthLevelPrefix(key, basePrefix string, level int) string {
	if level <= 0 || !strings.HasPrefix(key, basePrefix) {
		return ""
	}

	parts := strings.Split(strings.TrimPrefix(key, basePrefix), "/")
	if len(parts) < level {
		return ""
	}

	builder := strings.Builder{}
	builder.WriteString(strings.Trim(basePrefix, "/"))

	for i := 0; i < level; i++ {
		if part := strings.Trim(parts[i], "/"); part != "" {
			builder.WriteByte('/')
			builder.WriteString(part)
		}
	}

	return builder.String()
}
