package examples

import "context"

type BatchedDataSource[T any] interface {
	Collect(context.Context, string) ([]T, string, error)
}
