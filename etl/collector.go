package etl

import "context"

type DataCollector[T any] interface {
	Collect(context.Context) ([]T, error)
}
