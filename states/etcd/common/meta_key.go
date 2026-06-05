package common

import (
	"path"
	"strconv"
	"strings"
)

type MetaKeyPart string

const (
	KeyDatabaseID   MetaKeyPart = "databaseID"
	KeyCollectionID MetaKeyPart = "collectionID"
	KeyPartitionID  MetaKeyPart = "partitionID"
	KeySegmentID    MetaKeyPart = "segmentID"
	KeySnapshotID   MetaKeyPart = "snapshotID"
	KeyFieldID      MetaKeyPart = "fieldID"
	KeyIndexID      MetaKeyPart = "indexID"
	KeyJobID        MetaKeyPart = "jobID"
	KeyTaskID       MetaKeyPart = "taskID"
)

type MetaKeyHints map[MetaKeyPart]string

func NewMetaKeyHints() MetaKeyHints {
	return MetaKeyHints{}
}

func (h MetaKeyHints) WithInt64(part MetaKeyPart, value int64) MetaKeyHints {
	return h.WithInt64If(part, value, value > 0)
}

func (h MetaKeyHints) WithInt64If(part MetaKeyPart, value int64, ok bool) MetaKeyHints {
	if h == nil {
		h = NewMetaKeyHints()
	}
	if ok {
		h[part] = strconv.FormatInt(value, 10)
	}
	return h
}

type ScanTarget struct {
	Key   string
	Exact bool
}

type MetaKeySpec struct {
	Prefix string
	Parts  []MetaKeyPart
}

func (s MetaKeySpec) BuildScanTarget(basePath string, hints MetaKeyHints) ScanTarget {
	key := path.Join(basePath, s.Prefix)
	for _, part := range s.Parts {
		value, ok := hints[part]
		if !ok || value == "" {
			return ScanTarget{Key: ensureMetaPrefix(key), Exact: false}
		}
		key = path.Join(key, value)
	}
	if len(s.Parts) == 0 {
		return ScanTarget{Key: ensureMetaPrefix(key), Exact: false}
	}
	return ScanTarget{Key: key, Exact: true}
}

func ensureMetaPrefix(key string) string {
	if strings.HasSuffix(key, "/") {
		return key
	}
	return key + "/"
}
