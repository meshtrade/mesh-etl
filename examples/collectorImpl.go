package examples

import (
	"context"

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
func (d *SliceDataCollector) Collect(ctx context.Context) ([]string, error) {
	return d.data, nil
}
