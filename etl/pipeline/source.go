package pipeline

import (
	"context"
)

type Source[T any] func(ctx context.Context) ([]T, error)
