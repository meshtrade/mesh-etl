package pipeline

import (
	"context"

	"golang.org/x/sync/errgroup"
)

type sink[T any] func(context.Context, *PipelineState, chan T) error

func Emit[T any](emitFunc func(context.Context, *PipelineState, T) error) sink[T] {
	return func(ctx context.Context, p *PipelineState, inChannel chan T) error {
		// collect values from channel
		for inValue := range inChannel {
			if err := emitFunc(ctx, p, inValue); err != nil {
				return err
			}
		}

		return nil
	}
}

func Spread[T any](sinks ...sink[T]) sink[T] {
	// allocate list of channels for sinks
	sinkChannels := make([]chan T, len(sinks))

	// prepare err group
	var errGroup = new(errgroup.Group)

	return func(ctx context.Context, p *PipelineState, inChannel chan T) error {
		// collect input values from channel
		inValues := make([]T, len(inChannel))
		idx := 0
		for inValue := range inChannel {
			inValues[idx] = inValue
			idx++
		}

		// construct sink channel for each sink and load input values
		for range sinks {
			// allocate sink channel to hold values
			sinkChannel := make(chan T, len(inChannel))

			// load input values into new sink channel
			for _, inValue := range inValues {
				sinkChannel <- inValue
			}

			// close channel to indicate no more data will be sent
			close(sinkChannel)

			// add channel to list of channels
			sinkChannels = append(sinkChannels, sinkChannel)
		}

		// execute sinks concurrently
		for idx, sink := range sinks {
			errGroup.Go(func() error {
				if err := sink(ctx, p, sinkChannels[idx]); err != nil {
					return err
				}
				return nil
			})
		}

		// wait for sinks
		if err := errGroup.Wait(); err != nil {
			return err
		}

		return nil
	}
}

func SequenceSink[T any](
	sinks ...sink[T],
) sink[T] {
	return func(ctx context.Context, p *PipelineState, c chan T) error {
		for _, sink := range sinks {
			if err := sink(ctx, p, c); err != nil {
				return err
			}
		}
		return nil
	}
}
