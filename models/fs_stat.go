package models

import "time"

// FsStat model for file system statistics
type FsStat struct {
	Size         int64
	ETag         string
	LastModified time.Time
	VersionID    string
}
