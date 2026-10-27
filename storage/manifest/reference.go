package manifest

import (
	"encoding/json"
	"fmt"
	"path"
	"strings"
)

// Reference identifies an immutable manifest snapshot. Version 0 is the empty
// snapshot used before a segment's first commit; it has no manifest object.
type Reference struct {
	Version  int64  `json:"ver"`
	BasePath string `json:"base_path"`
}

func ParseReference(raw string) (Reference, error) {
	var value struct {
		Version  *int64 `json:"ver"`
		BasePath string `json:"base_path"`
	}
	if err := json.Unmarshal([]byte(raw), &value); err != nil {
		return Reference{}, fmt.Errorf("parse manifest reference: %w", err)
	}
	if value.Version == nil || *value.Version < 0 || strings.TrimSpace(value.BasePath) == "" {
		return Reference{}, fmt.Errorf("manifest reference requires a nonnegative ver and a nonempty base_path")
	}
	return Reference{Version: *value.Version, BasePath: strings.TrimSpace(value.BasePath)}, nil
}

func (r Reference) ObjectPath() string {
	return path.Join(r.BasePath, "_metadata", fmt.Sprintf("manifest-%d.avro", r.Version))
}

// FilePath resolves internal paths stored relative to their manifest section.
// URI and ROOT_PATH handling belongs to the caller's object-store resolver.
func (r Reference) FilePath(section, name string) (string, error) {
	if strings.TrimSpace(name) == "" {
		return "", fmt.Errorf("manifest file path is empty")
	}
	if strings.Contains(name, "://") || strings.Contains(name, "ROOT_PATH") {
		return name, nil
	}
	return path.Join(r.BasePath, section, name), nil
}
