package pipeline

import "context"

type Pipeline[T, V any] struct {
	source source[T]
	stage  stage[T, V]
	sink   sink[V]
	state  *PipelineState
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
		state:  NewPipelineState(),
	}
}

func (p *Pipeline[T, V]) Execute(ctx context.Context) error {
	sourceChannel, err := p.source(ctx, p.state)
	if err != nil {
		return err
	}

	stageChannel, err := p.stage(ctx, p.state, sourceChannel)
	if err != nil {
		return err
	}

	if err := p.sink(ctx, p.state, stageChannel); err != nil {
		return err
	}

	return p.state.RunAfterEffects(ctx)
}
