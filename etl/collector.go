package etl

import "context"

type DataCollector[T any] interface {
	Collect(ctx context.Context, pagingToken string) ([]T, string, error)
}
