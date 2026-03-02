package healthz

import (
	"testing"
)

func TestGrantAliasCheckRegistered(t *testing.T) {
	item, ok := GetHealthzCheckItem("GRANT_ALIAS_CHECK")
	if !ok {
		t.Fatal("GRANT_ALIAS_CHECK not registered in allCheckItems")
	}
	if item.Name() != "GRANT_ALIAS_CHECK" {
		t.Fatalf("expected name GRANT_ALIAS_CHECK, got %s", item.Name())
	}
	if item.Description() == "" {
		t.Fatal("description should not be empty")
	}
	t.Logf("GRANT_ALIAS_CHECK registered: %s", item.Description())

	// Verify total count includes the new item
	all := AllCheckItems()
	found := false
	for _, i := range all {
		if i.Name() == "GRANT_ALIAS_CHECK" {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("GRANT_ALIAS_CHECK not found in AllCheckItems()")
	}
	t.Logf("Total healthz check items: %d", len(all))
}
