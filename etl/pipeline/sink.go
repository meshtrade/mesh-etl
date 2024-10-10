package pipeline

import "context"

type Sink[T any] func(context.Context, []T) error
