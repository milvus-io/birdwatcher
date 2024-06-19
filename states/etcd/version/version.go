package version

var currentVersion string

// SetVersion set current version manually.
func SetVersion(ver string) {
	currentVersion = ver
}

// GetVersion returns current etcd version.
// tmp solution before env supported.
func GetVersion() string {
	return currentVersion
}
