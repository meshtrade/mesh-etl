package pipeline

import (
	"context"
)

type source[T any] func(ctx context.Context) (chan T, error)
type chainedSource[T, V any] func(context.Context, chan T) (chan V, error)

func SourceBatch[T any](sourceFunc func(context.Context) ([]T, error)) source[T] {
	// return function to be executed during pipeline execution
	return func(ctx context.Context) (chan T, error) {
		// execute source
		batch, err := sourceFunc(ctx)
		if err != nil {
			return nil, err
		}

		// create batch channel
		batchChan := make(chan T, len(batch))

		// load batch into channel into channel if nothing went wrong
		for _, element := range batch {
			batchChan <- element
		}
		close(batchChan)

		return batchChan, nil
	}
}

func ChainedSourceBatch[T, V any](sourceFunc func(context.Context, []T) ([]V, error)) chainedSource[T, V] {
	return func(ctx context.Context, inChannel chan T) (chan V, error) {
		inValues := make([]T, len(inChannel))
		idx := 0
		for inValue := range inChannel {
			inValues[idx] = inValue
			idx++
		}

		outValues, err := sourceFunc(ctx, inValues)
		if err != nil {
			return nil, err
		}

		outChannel := make(chan V, len(outValues))
		for _, outValue := range outValues {
			outChannel <- outValue
		}
		close(outChannel)

		return outChannel, nil
	}
}

func SourceScalar[T any](source func(context.Context) (T, error)) source[T] {
	// return function to be executed during pipeline execution
	return func(ctx context.Context) (chan T, error) {
		// create synchronous channel
		scalarChan := make(chan T, 1)

		// execute source
		scalar, err := source(ctx)
		if err != nil {
			return nil, err
		}

		// load scalar value into channel if nothing went wrong
		scalarChan <- scalar
		close(scalarChan)

		return scalarChan, err
	}
}

func ChainedSourceScalar[T, V any](sourceFunc func(context.Context, T) ([]V, error)) chainedSource[T, V] {
	return func(ctx context.Context, inChannel chan T) (chan V, error) {
		// reading all values to prevent leak
		inValues := make([]T, len(inChannel))
		idx := 0
		for inValue := range inChannel {
			inValues[idx] = inValue
			idx++
		}

		// call source with first inValue
		outValues, err := sourceFunc(ctx, inValues[0])
		if err != nil {
			return nil, err
		}

		// collect output
		outChannel := make(chan V, len(outValues))
		for _, outValue := range outValues {
			outChannel <- outValue
		}
		close(outChannel)

		return outChannel, nil
	}
}

func SequenceSource[T, V any](source1 source[T], source2 chainedSource[T, V]) source[V] {
	return func(ctx context.Context) (chan V, error) {
		// execute source1 to obtain handle to channel
		source1Chan, err := source1(ctx)
		if err != nil {
			return nil, err
		}

		// execute source2 given source1
		source2Chan, err := source2(ctx, source1Chan)

		// load source2 data into channel
		chainChannel := make(chan V, len(source2Chan))
		for source2Value := range source2Chan {
			chainChannel <- source2Value
		}
		close(chainChannel)

		return chainChannel, err
	}
}
