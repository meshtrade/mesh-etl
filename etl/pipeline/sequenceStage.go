package pipeline

import (
	"context"
)

func Sequence[T, V, K any](
	s1 Stage[T, V],
	s2 Stage[V, K],
) Stage[T, K] {
	return func(ctx context.Context, t []T) ([]K, error) {
		ks, err := s1(ctx, t)
		if err != nil {
			return nil, err
		}
		return s2(ctx, ks)
	}
}
