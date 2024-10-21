package pipeline

import "context"

type sink[T any] func(context.Context, chan T) error

func Emit[T any](emitFunc func(context.Context, T) error) sink[T] {
	return func(ctx context.Context, inChannel chan T) error {
		// collect values from channel
		for inValue := range inChannel {
			if err := emitFunc(ctx, inValue); err != nil {
				return err
			}
		}

		return nil
	}
}

func Spread[T any](sinks ...sink[T]) sink[T] {
	// create list of channels for sinks
	sinkChannels := make([]chan T, len(sinks))

	return func(ctx context.Context, inChannel chan T) error {
		// collect input values from channel
		inValues := make([]T, len(inChannel))
		idx := 0
		for inValue := range inChannel {
			inValues[idx] = inValue
			idx++
		}

		// construct sink channel for each sink and load input values
		for range sinks {
			sinkChannel := make(chan T, len(inChannel))
			for _, inValue := range inValues {
				sinkChannel <- inValue
			}
			close(sinkChannel)
			sinkChannels = append(sinkChannels, sinkChannel)
		}

		// execute sinks
		for idx, sink := range sinks {
			if err := sink(ctx, sinkChannels[idx]); err != nil {
				return err
			}
		}

		return nil
	}
}

func SequenceSink[T any](
	sinks ...sink[T],
) sink[T] {
	return func(ctx context.Context, c chan T) error {
		for _, sink := range sinks {
			if err := sink(ctx, c); err != nil {
				return err
			}
		}
		return nil
	}
}
