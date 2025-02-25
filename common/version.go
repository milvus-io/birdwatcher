package common

import "github.com/blang/semver/v4"

// Version current version for session
var Version semver.Version

func init() {
	Version = semver.MustParse("1.0.8")
}
