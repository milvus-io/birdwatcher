package show

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	etcdversion "github.com/milvus-io/birdwatcher/states/etcd/version"
)

type GrantAliasCheckParam struct {
	framework.ParamBase `use:"show grant-alias-check" desc:"check if any RBAC grants are using alias names instead of real collection names"`
}

// GrantAliasCheckCommand implements `show grant-alias-check` command.
func (c *ComponentShow) GrantAliasCheckCommand(ctx context.Context, p *GrantAliasCheckParam) (*GrantAliasCheckResult, error) {
	// 1. Load all aliases
	aliases, err := common.ListAlias(ctx, c.client, c.metaPath, etcdversion.GetVersion(), func(a *models.Alias) bool {
		return a.State == models.AliasStateAliasCreated
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to list aliases")
	}

	// Build alias name set (key = "dbID:aliasName")
	// Note: alias model has DBID (int64) but grant has dbName (string),
	// so we just use alias name for matching since alias names are unique within a database.
	aliasNames := make(map[string]int64) // aliasName -> collectionID
	for _, a := range aliases {
		aliasNames[a.Name] = a.CollectionID
	}

	if len(aliasNames) == 0 {
		return &GrantAliasCheckResult{}, nil
	}

	// 2. Load all grants
	grants, err := common.ListGrantEntries(ctx, c.client, c.metaPath)
	if err != nil {
		return nil, errors.Wrap(err, "failed to list grants")
	}

	// 3. Cross-check: find grants whose objectName is an alias
	var matches []AliasGrantMatch
	for _, g := range grants {
		if g.ObjectType != "Collection" {
			continue
		}
		if g.ObjectName == "*" {
			continue
		}
		if collID, ok := aliasNames[g.ObjectName]; ok {
			matches = append(matches, AliasGrantMatch{
				Role:         g.Role,
				DBName:       g.DBName,
				AliasName:    g.ObjectName,
				CollectionID: collID,
			})
		}
	}

	return &GrantAliasCheckResult{Matches: matches}, nil
}

type AliasGrantMatch struct {
	Role         string
	DBName       string
	AliasName    string
	CollectionID int64
}

type GrantAliasCheckResult struct {
	Matches []AliasGrantMatch
}

func (r *GrantAliasCheckResult) Entities() any {
	return r.Matches
}

func (r *GrantAliasCheckResult) PrintAs(format framework.Format) string {
	switch format {
	case framework.FormatDefault, framework.FormatPlain:
		sb := &strings.Builder{}
		if len(r.Matches) == 0 {
			fmt.Fprintln(sb, "No grants found on alias names. Safe to enable proxy.resolveAliasForPrivilege.")
			return sb.String()
		}

		fmt.Fprintf(sb, "Found %d grant(s) on alias names:\n", len(r.Matches))
		fmt.Fprintln(sb, "These grants need to be migrated to real collection names before enabling proxy.resolveAliasForPrivilege.")
		fmt.Fprintln(sb, "---")
		for _, m := range r.Matches {
			fmt.Fprintf(sb, "  Role: %s, DB: %s, Alias: %s (-> CollectionID: %d)\n",
				m.Role, m.DBName, m.AliasName, m.CollectionID)
		}
		return sb.String()
	default:
	}
	return ""
}
