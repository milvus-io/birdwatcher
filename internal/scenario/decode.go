package scenario

import "gopkg.in/yaml.v3"

// copyNode returns a deep copy of a yaml.Node tree so interpolation does
// not mutate the parsed scenario document across re-runs.
func copyNode(n *yaml.Node) *yaml.Node {
	if n == nil {
		return nil
	}
	cp := *n
	if len(n.Content) > 0 {
		cp.Content = make([]*yaml.Node, len(n.Content))
		for i, c := range n.Content {
			cp.Content[i] = copyNode(c)
		}
	}
	return &cp
}

// isEmptyNode reports whether the node has no useful content. Used to
// skip Decode when a step omits `params:`.
func isEmptyNode(n *yaml.Node) bool {
	if n == nil {
		return true
	}
	if n.Kind == 0 {
		return true
	}
	if n.Kind == yaml.ScalarNode && n.Value == "" {
		return true
	}
	if n.Kind == yaml.MappingNode && len(n.Content) == 0 {
		return true
	}
	return false
}
