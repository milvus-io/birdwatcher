package ops

import (
	"fmt"
	"strings"

	"github.com/milvus-io/milvus/client/v2/entity"
)

func errNoClient(op string) error {
	return fmt.Errorf("%s: no Milvus client (did the scenario forget to connect?)", op)
}

// parseConsistency maps human strings to entity.ConsistencyLevel. Returns
// (_, false) when the input is empty or unknown so callers can skip
// applying a level.
func parseConsistency(s string) (entity.ConsistencyLevel, bool) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "default":
		return 0, false
	case "strong":
		return entity.ClStrong, true
	case "session":
		return entity.ClSession, true
	case "bounded":
		return entity.ClBounded, true
	case "eventually":
		return entity.ClEventually, true
	case "customized":
		return entity.ClCustomized, true
	}
	return 0, false
}
