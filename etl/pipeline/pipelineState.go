package pipeline

import (
	"context"

	"golang.org/x/sync/errgroup"
)

type AfterEffect func(ctx context.Context) error

type PipelineState struct {
	afterEffects []AfterEffect
}

func NewPipelineState() *PipelineState {
	return &PipelineState{
		afterEffects: []AfterEffect{},
	}
}

func (p *PipelineState) RegisterAfterEffect(afterEffect AfterEffect) {
	p.afterEffects = append(p.afterEffects, afterEffect)
}

func (p *PipelineState) RunAfterEffects(ctx context.Context) error {
	if len(p.afterEffects) == 0 {
		return nil
	}

	errGroup := new(errgroup.Group)
	for _, afterEffect := range p.afterEffects {
		errGroup.Go(func() error {
			return afterEffect(ctx)
		})
	}

	if err := errGroup.Wait(); err != nil {
		return err
	}

	p.afterEffects = []AfterEffect{}

	return nil
}
