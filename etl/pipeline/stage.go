package pipeline

import (
	"context"
)

type Stage[T any, V any] func(context.Context, []T) ([]V, error)
