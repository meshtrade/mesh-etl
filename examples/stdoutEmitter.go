package examples

import (
	"context"
	"fmt"

	"github.com/meshtrade/mesh-etl/etl"
)

var _ etl.DataEmitter = &StdOutEmitter{}

type StdOutEmitter struct {
}

func NewSTDOutEmitter() *StdOutEmitter {
	return &StdOutEmitter{}
}

func (e *StdOutEmitter) Emit(ctx context.Context, data []byte) error {
	fmt.Printf("Data: %v\n", data)
	return nil
}
