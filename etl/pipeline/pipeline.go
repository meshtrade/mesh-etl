package pipeline

import "context"

type Pipeline[T, V any] struct {
	source source[T]
	stage  stage[T, V]
	sink   sink[V]
}

func NewPipeline[T, V any](
	source source[T],
	stage stage[T, V],
	sink sink[V],
) *Pipeline[T, V] {
	return &Pipeline[T, V]{
		source: source,
		stage:  stage,
		sink:   sink,
	}
}

func (p *Pipeline[T, V]) Execute(ctx context.Context) error {
	sourceChannel, err := p.source(ctx)
	if err != nil {
		return err
	}

	stageChannel, err := p.stage(ctx, sourceChannel)
	if err != nil {
		return err
	}

	return p.sink(ctx, stageChannel)
}
