package pipeline

import "context"

func SequenceSource[T, V any](
	s1 Source[T],
	s2 ChainedSource[T, V],
) Source[V] {
	return func(ctx context.Context) ([]V, error) {
		ts, err := s1(ctx)
		if err != nil {
			return nil, err
		}
		return s2(ctx, ts)(ctx)
	}
}

func SequenceStage[T, V, K any](
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

func SequenceSink[T any](
	sinks ...Sink[T],
) Sink[T] {
	return func(ctx context.Context, t []T) error {
		for _, sink := range sinks {
			if err := sink(ctx, t); err != nil {
				return err
			}
		}
		return nil
	}
}
