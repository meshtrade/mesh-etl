package pipeline

import "context"

type Pipeline[T, V any] struct {
	source Source[T]
	stage  Stage[T, V]
	sink   Sink[V]
}

func NewPipeline[T, V any](
	source Source[T],
	stage Stage[T, V],
	sink Sink[V],
) *Pipeline[T, V] {
	return &Pipeline[T, V]{
		source: source,
		stage:  stage,
		sink:   sink,
	}
}

func (p *Pipeline[T, V]) Execute(ctx context.Context) error {
	values, err := p.source(ctx)
	if err != nil {
		return err
	}

	stageValues, err := p.stage(ctx, values)
	if err != nil {
		return err
	}

	return p.sink(ctx, stageValues)
}
