package autocomplete

import "sync"

// ValueSuggester provides dynamic value suggestions for a flag.
type ValueSuggester interface {
	Suggest(partial string) []string
}

// ValueSuggestFunc adapts a simple function to ValueSuggester.
type ValueSuggestFunc func(partial string) []string

// Suggest implements ValueSuggester.
func (f ValueSuggestFunc) Suggest(partial string) []string { return f(partial) }

// ValueSuggestionContext contains values already supplied for sibling flags.
// Context-aware suggesters can use them to resolve dependent resources, such
// as fields within the collection selected by --collection.
type ValueSuggestionContext struct {
	FlagValues map[string]string
}

// ContextValueSuggester extends ValueSuggester with sibling flag values while
// preserving compatibility with existing prefix-only suggesters.
type ContextValueSuggester interface {
	ValueSuggester
	SuggestWithContext(partial string, ctx ValueSuggestionContext) []string
}

// ContextValueSuggestFunc adapts a context-aware suggestion function.
type ContextValueSuggestFunc func(partial string, ctx ValueSuggestionContext) []string

// Suggest implements ValueSuggester for callers without suggestion context.
func (f ContextValueSuggestFunc) Suggest(partial string) []string {
	return f(partial, ValueSuggestionContext{})
}

// SuggestWithContext implements ContextValueSuggester.
func (f ContextValueSuggestFunc) SuggestWithContext(partial string, ctx ValueSuggestionContext) []string {
	return f(partial, ctx)
}

var (
	suggestRegistry = map[string]ValueSuggester{}
	mutex           sync.RWMutex
)

// RegisterValueSuggester registers a named ValueSuggester.
func RegisterValueSuggester(name string, s ValueSuggester) {
	mutex.Lock()
	defer mutex.Unlock()
	suggestRegistry[name] = s
}

// GetValueSuggester looks up a registered ValueSuggester by name.
func GetValueSuggester(name string) (ValueSuggester, bool) {
	mutex.RLock()
	defer mutex.RUnlock()
	v, ok := suggestRegistry[name]
	return v, ok
}

// UnregisterValueSuggester removes a named ValueSuggester from the registry.
func UnregisterValueSuggester(name string) {
	mutex.Lock()
	defer mutex.Unlock()
	delete(suggestRegistry, name)
}
