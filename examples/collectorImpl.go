package examples

import (
	"context"
	"fmt"
	"strconv"

	"github.com/meshtrade/mesh-etl/etl"
)

// ensure slice data collector implements interface
var _ etl.DataCollector[string] = &SliceDataCollector{}

// SliceDataCollector is used to collect data from an internal slice of string
type SliceDataCollector struct {
	data      []string
	batchSize int
}

func NewSliceDataCollector(_data []string, _batchSize int) *SliceDataCollector {
	return &SliceDataCollector{
		data:      _data,
		batchSize: _batchSize,
	}
}

// Collect implements etl.DataCollector.
func (d *SliceDataCollector) Collect(ctx context.Context, pagingToken string) ([]string, string, error) {
	// interpret paging token as integer to index into slice
	idx, err := strconv.Atoi(pagingToken)
	if err != nil {
		return nil, "", fmt.Errorf("could not interpret paging token as int")
	}

	// collect batch data
	batch := d.data[idx : idx+d.batchSize]

	return batch, string(idx + d.batchSize), nil
}
