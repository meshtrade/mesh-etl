package pipeline

import "context"

type JoinType int

const (
	JoinLeft JoinType = iota
	JoinRight
	JoinLeftSingle
	JoinRightSingle
)

type JoinFunc[T, V, K any] func(ctx context.Context, left T, right V) K

func JoinSource[T, V, K any](
	left Source[T],
	right Source[V],
	joinFunc JoinFunc[T, V, K],
) Source[K] {
	return func(ctx context.Context) ([]K, error) {
		leftValues, err := left(ctx)
		if err != nil {
			return nil, err
		}
		rightValues, err := right(ctx)
		if err != nil {
			return nil, err
		}

		joinType := validate(
			leftValues,
			rightValues,
		)

		switch joinType {
		case JoinLeft:
			return joinLeft(
				ctx,
				leftValues,
				rightValues,
				joinFunc,
			), nil
		case JoinLeftSingle:
			return joinLeftSingle(
				ctx,
				leftValues,
				rightValues,
				joinFunc,
			), nil
		case JoinRight:
			return joinRight(
				ctx,
				leftValues,
				rightValues,
				joinFunc,
			), nil
		case JoinRightSingle:
			return joinRightSingle(
				ctx,
				leftValues,
				rightValues,
				joinFunc,
			), nil
		}

		return nil, nil
	}
}

func validate[T, V any](left []T, right []V) JoinType {
	leftLen := len(left)
	rightLen := len(right)

	if leftLen == 1 {
		return JoinLeftSingle
	}

	if rightLen == 1 {
		return JoinRightSingle
	}

	if leftLen > rightLen {
		return JoinLeft
	}

	return JoinRight
}

func joinLeft[T, V, K any](ctx context.Context, left []T, right []V, joinFunc JoinFunc[T, V, K]) []K {
	ks := []K{}
	for rightIdx, rightValue := range right {
		ks = append(ks, joinFunc(ctx, left[rightIdx], rightValue))
	}
	return ks
}

func joinLeftSingle[T, V, K any](ctx context.Context, left []T, right []V, joinFunc JoinFunc[T, V, K]) []K {
	ks := []K{}
	for _, rightValue := range right {
		ks = append(ks, joinFunc(ctx, left[0], rightValue))
	}
	return ks
}

func joinRight[T, V, K any](ctx context.Context, left []T, right []V, joinFunc JoinFunc[T, V, K]) []K {
	ks := []K{}
	for leftIdx, leftValue := range left {
		ks = append(ks, joinFunc(ctx, leftValue, right[leftIdx]))
	}
	return ks
}

func joinRightSingle[T, V, K any](ctx context.Context, left []T, right []V, joinFunc JoinFunc[T, V, K]) []K {
	ks := []K{}
	for _, leftValue := range left {
		ks = append(ks, joinFunc(ctx, leftValue, right[0]))
	}
	return ks
}
