package pipeline

import (
	"context"
)

type Source[T any] func(ctx context.Context) ([]T, error)

type ChainedSource[T, V any] func(context.Context, []T) func(context.Context) ([]V, error)
