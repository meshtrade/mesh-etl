package pipeline

import "context"

func Spread[T any](sinks ...Sink[T]) func(ctx context.Context, t []T) error {
	return func(ctx context.Context, t []T) error {
		for _, sink := range sinks {
			if err := sink(ctx, t); err != nil {
				return err
			}
		}
		return nil
	}
}
