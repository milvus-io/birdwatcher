package autocomplete

// ValueSuggester provides dynamic value suggestions for a flag.
type ValueSuggester interface {
	Suggest(partial string) []string
}

// ValueSuggestFunc adapts a simple function to ValueSuggester.
type ValueSuggestFunc func(partial string) []string

// Suggest implements ValueSuggester.
func (f ValueSuggestFunc) Suggest(partial string) []string { return f(partial) }

var suggestRegistry = map[string]ValueSuggester{}

// RegisterValueSuggester registers a named ValueSuggester.
func RegisterValueSuggester(name string, s ValueSuggester) { suggestRegistry[name] = s }

// GetValueSuggester looks up a registered ValueSuggester by name.
func GetValueSuggester(name string) (ValueSuggester, bool) {
	v, ok := suggestRegistry[name]
	return v, ok
}

// UnregisterValueSuggester removes a named ValueSuggester from the registry.
func UnregisterValueSuggester(name string) { delete(suggestRegistry, name) }
