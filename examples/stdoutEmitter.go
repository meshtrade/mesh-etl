package main

import (
	"context"
	"fmt"

	"github.com/meshtrade/mesh-etl/etl/pipeline"
)

type StdOutEmitter[T any] struct {
}

func NewSTDOutEmitter[T any]() *StdOutEmitter[T] {
	return &StdOutEmitter[T]{}
}

func (e *StdOutEmitter[T]) Emit(ctx context.Context, pipelineState *pipeline.PipelineState, value T) error {
	fmt.Printf("Data: %v\n", value)
	return nil
}
