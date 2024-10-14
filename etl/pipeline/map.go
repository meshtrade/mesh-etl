package pipeline

import "context"

func Map[T, V any](mapFunc func(context.Context, T) V) Stage[T, V] {
	return func(ctx context.Context, t []T) ([]V, error) {
		mappedArr := []V{}
		for _, value := range t {
			mappedArr = append(mappedArr, mapFunc(ctx, value))
		}
		return mappedArr, nil
	}
}
