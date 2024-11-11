package pipeline

import (
	"context"
	"math/rand"
)

type stage[T any, V any] func(context.Context, *PipelineState, chan T) (chan V, error)

func Map[T, V any](mapFunc func(context.Context, *PipelineState, T) V) stage[T, V] {
	return func(ctx context.Context, p *PipelineState, inChannel chan T) (chan V, error) {
		// create channel with buffer for each element in inChannel
		mapChan := make(chan V, len(inChannel))
		defer close(mapChan)

		// map values from inChannel
		for inValue := range inChannel {
			mapChan <- mapFunc(ctx, p, inValue)
		}

		return mapChan, nil
	}
}

func MapError[T, V any](mapFunc func(context.Context, *PipelineState, T) (V, error)) stage[T, V] {
	return func(ctx context.Context, p *PipelineState, inChannel chan T) (chan V, error) {
		// create channel with buffer for each element in inChannel
		mapChan := make(chan V, len(inChannel))

		// map values from inChannel
		for inValue := range inChannel {
			value, err := mapFunc(ctx, p, inValue)
			if err != nil {
				return nil, err
			}
			mapChan <- value
		}
		close(mapChan)

		return mapChan, nil
	}
}

func Filter[T any](filterFunc func(context.Context, *PipelineState, T) bool) stage[T, T] {
	return func(ctx context.Context, p *PipelineState, inChannel chan T) (chan T, error) {
		// optimistically allocate buffer for all elements in input channel
		filterChan := make(chan T, len(inChannel))
		defer close(filterChan)

		// filter values from inChannel
		for inValue := range inChannel {
			if filterFunc(ctx, p, inValue) {
				filterChan <- inValue
			}
		}

		return filterChan, nil
	}
}

func Shuffle[T any]() stage[T, T] {
	return func(ctx context.Context, p *PipelineState, input chan T) (chan T, error) {
		// load values from input into array
		inputValues := make([]T, len(input))
		idx := 0
		for inputValue := range input {
			inputValues[idx] = inputValue
			idx++
		}

		// shuffle the data (NOTE: Assuming Go 1.20 runtime which automatically sets seed)
		rand.Shuffle(len(inputValues), func(i, j int) {
			inputValues[i], inputValues[j] = inputValues[j], inputValues[i]
		})

		// load data into return channel
		outChannel := make(chan T, len(inputValues))
		defer close(outChannel)
		for _, inputValue := range inputValues {
			outChannel <- inputValue
		}

		return outChannel, nil
	}
}

func Count[T comparable]() stage[T, map[T]int] {
	return func(ctx context.Context, p *PipelineState, inChannel chan T) (chan map[T]int, error) {
		countMap := make(map[T]int)

		for inValue := range inChannel {
			_, found := countMap[inValue]
			if found {
				countMap[inValue] += 1
			} else {
				countMap[inValue] = 1
			}
		}

		outputChan := make(chan map[T]int, 1)
		outputChan <- countMap
		close(outputChan)

		return outputChan, nil
	}
}

func SequenceStage[T, V, K any](
	stage1 stage[T, V],
	stage2 stage[V, K],
) stage[T, K] {
	return func(ctx context.Context, p *PipelineState, inChannel chan T) (chan K, error) {
		// execute stage 1
		stage1Channel, err := stage1(ctx, p, inChannel)
		if err != nil {
			return nil, err
		}

		// execute stage 2
		stage2Channel, err := stage2(ctx, p, stage1Channel)
		if err != nil {
			return nil, err
		}

		return stage2Channel, nil
	}
}

func SequenceStage3[T, V, K, M any](
	stage1 stage[T, V],
	stage2 stage[V, K],
	stage3 stage[K, M],
) stage[T, M] {
	return func(ctx context.Context, p *PipelineState, inChannel chan T) (chan M, error) {
		// construct sequenced for stages 1 + 2
		sequencedStage1 := SequenceStage(stage1, stage2)

		// execute stages 1 + 2
		sequencedChan, err := sequencedStage1(ctx, p, inChannel)
		if err != nil {
			return nil, err
		}

		// execute sequenced stage 3
		stage3Chan, err := stage3(ctx, p, sequencedChan)
		if err != nil {
			return nil, err
		}

		return stage3Chan, nil
	}
}

func SequenceStage4[T, V, K, M, L any](
	stage1 stage[T, V],
	stage2 stage[V, K],
	stage3 stage[K, M],
	stage4 stage[M, L],
) stage[T, L] {
	return func(ctx context.Context, p *PipelineState, inChannel chan T) (chan L, error) {
		// construct sequenced for stages 1 + 2
		sequencedStage1 := SequenceStage3(stage1, stage2, stage3)

		// execute stages 1 + 2 + 3
		sequencedChan, err := sequencedStage1(ctx, p, inChannel)
		if err != nil {
			return nil, err
		}

		// execute sequenced stage 4
		stage4Chan, err := stage4(ctx, p, sequencedChan)
		if err != nil {
			return nil, err
		}

		return stage4Chan, nil
	}
}
