package main

import "context"

type DataEmitter[T any] interface {
	Emit(ctx context.Context, value T) error
}
