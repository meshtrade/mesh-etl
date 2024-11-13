package main

import (
	"context"
	"fmt"
	"log"

	"github.com/meshtrade/mesh-etl/etl/pipeline"
)

type Model struct {
	Value int
}

func main() {
	pipeline := pipeline.NewPipeline(
		pipeline.SequenceSource(
			pipeline.SourceScalar(func(ctx context.Context, ps *pipeline.PipelineState) (int, error) {
				ps.RegisterAfterEffect(func(ctx context.Context) error {
					fmt.Println("Got return value: ", 1)
					return nil
				})
				return 1, nil
			}),
			pipeline.ChainedSourceScalar(func(ctx context.Context, p *pipeline.PipelineState, input int) ([]int, error) {
				return []int{}, nil
			}),
		),
		pipeline.Map(func(ctx context.Context, p *pipeline.PipelineState, input int) int {
			return 0
		}),
		pipeline.Emit(func(ctx context.Context, p *pipeline.PipelineState, input int) error {
			return nil
		}),
	)

	if err := pipeline.Execute(context.Background()); err != nil {
		log.Fatal(err)
	}
}
