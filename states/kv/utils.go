package kv

import (
	"path"
	"strings"
)

func joinPath(parts ...string) string {
	r := path.Join(parts...)
	if len(parts) > 0 && strings.HasSuffix(parts[len(parts)-1], "/") {
		r += "/"
	}
	return r
}
