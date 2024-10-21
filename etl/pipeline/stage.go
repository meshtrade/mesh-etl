package pipeline

import (
	"context"
)

type stage[T any, V any] func(context.Context, chan T) (chan V, error)

func Map[T, V any](mapFunc func(context.Context, T) V) stage[T, V] {
	return func(ctx context.Context, inChannel chan T) (chan V, error) {
		// create channel with buffer for each element in inChannel
		mapChan := make(chan V, len(inChannel))

		// map values from inChannel
		for inValue := range inChannel {
			mapChan <- mapFunc(ctx, inValue)
		}
		close(mapChan)

		return mapChan, nil
	}
}

func Filter[T any](filterFunc func(context.Context, T) bool) stage[T, T] {
	return func(ctx context.Context, inChannel chan T) (chan T, error) {
		// optimistically allocate buffer for all elements in input channel
		filterChan := make(chan T, len(inChannel))

		// filter values from inChannel
		for inValue := range inChannel {
			if filterFunc(ctx, inValue) {
				filterChan <- inValue
			}
		}

		return filterChan, nil
	}
}

func SequenceStage[T, V, K any](
	stage1 stage[T, V],
	stage2 stage[V, K],
) stage[T, K] {
	return func(ctx context.Context, inChannel chan T) (chan K, error) {

		// execute stage 1
		stage1Channel, err := stage1(ctx, inChannel)
		if err != nil {
			return nil, err
		}

		// execute stage 2
		stage2Channel, err := stage2(ctx, stage1Channel)
		if err != nil {
			return nil, err
		}

		return stage2Channel, err
	}
}
