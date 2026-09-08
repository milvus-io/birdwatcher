//go:build !LANCE || !cgo

package states

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

func ensureLanceVectorScannerAvailable() error {
	return errors.New(
		"Lance vector scanning is not available in this binary; build Birdwatcher with CGO_ENABLED=1 and -tags LANCE against milvus-storage",
	)
}

func scanLanceVectorNullRanges(
	context.Context,
	[]externalVectorSegmentRange,
	string,
	schemapb.DataType,
	int64,
	int,
	int64,
	externalSourceLocation,
	externalSourceSpec,
) <-chan externalVectorObjectResult {
	results := make(chan externalVectorObjectResult)
	close(results)
	return results
}
