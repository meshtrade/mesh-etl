package main

import (
	"context"
	"fmt"
)

type StdOutEmitter[T any] struct {
}

func NewSTDOutEmitter[T any]() *StdOutEmitter[T] {
	return &StdOutEmitter[T]{}
}

func (e *StdOutEmitter[T]) Emit(ctx context.Context, value T) error {
	fmt.Printf("Data: %v\n", value)
	return nil
}
