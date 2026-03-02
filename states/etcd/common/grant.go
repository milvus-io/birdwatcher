package common

import (
	"context"
	"path"
	"strings"

	"github.com/milvus-io/birdwatcher/states/kv"
)

const (
	// GranteePrefix is the etcd key prefix for grant entries.
	// Key format: root-coord/credential/grantee-privileges/{tenant}/{role}/{objectType}/{dbName.objectName}
	GranteePrefix = `root-coord/credential/grantee-privileges`
)

// GrantEntry represents a parsed grant from etcd key.
type GrantEntry struct {
	Role       string
	ObjectType string
	DBName     string
	ObjectName string // could be a collection name or alias name
}

// ListGrantEntries reads all grantee-privileges keys from etcd and parses them.
func ListGrantEntries(ctx context.Context, cli kv.MetaKV, basePath string) ([]GrantEntry, error) {
	prefix := path.Join(basePath, GranteePrefix)
	keys, _, err := cli.LoadWithPrefix(ctx, prefix)
	if err != nil {
		return nil, err
	}

	var entries []GrantEntry
	for _, key := range keys {
		// Strip basePath prefix to get: root-coord/credential/grantee-privileges/{tenant}/{role}/{objectType}/{dbName.objectName}
		rel := strings.TrimPrefix(key, prefix+"/")
		parts := strings.SplitN(rel, "/", 4)
		// parts[0] = tenant (empty or tenant name)
		// parts[1] = role
		// parts[2] = objectType
		// parts[3] = dbName.objectName (or just objectName for legacy)
		if len(parts) < 4 {
			// Try without tenant: role/objectType/dbName.objectName
			parts = strings.SplitN(rel, "/", 3)
			if len(parts) < 3 {
				continue
			}
			entry := GrantEntry{
				Role:       parts[0],
				ObjectType: parts[1],
			}
			entry.DBName, entry.ObjectName = splitObjectName(parts[2])
			entries = append(entries, entry)
			continue
		}

		entry := GrantEntry{
			Role:       parts[1],
			ObjectType: parts[2],
		}
		entry.DBName, entry.ObjectName = splitObjectName(parts[3])
		entries = append(entries, entry)
	}

	return entries, nil
}

// splitObjectName splits "dbName.objectName" into dbName and objectName.
// If there's no dot, returns "default" as dbName.
func splitObjectName(combined string) (string, string) {
	idx := strings.Index(combined, ".")
	if idx < 0 {
		return "default", combined
	}
	return combined[:idx], combined[idx+1:]
}
