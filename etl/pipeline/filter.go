package pipeline

import (
	"context"
)

func Filter[T any](filterFunc func(context.Context, T) bool) Stage[T, T] {
	return func(ctx context.Context, t []T) ([]T, error) {
		filteredArr := []T{}
		for _, value := range t {
			if filterFunc(ctx, value) {
				filteredArr = append(filteredArr, value)
			}
		}
		return filteredArr, nil
	}
}

func Map[T, V any](mapFunc func(context.Context, T) V) Stage[T, V] {
	return func(ctx context.Context, t []T) ([]V, error) {
		mappedArr := []V{}
		for _, value := range t {
			mappedArr = append(mappedArr, mapFunc(ctx, value))
		}
		return mappedArr, nil
	}
}
