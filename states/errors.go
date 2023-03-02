package states

import "github.com/cockroachdb/errors"

// ErrPathIsDir error indicating path shall be file but a directory.
var ErrPathIsDir = errors.New("path is directory")
