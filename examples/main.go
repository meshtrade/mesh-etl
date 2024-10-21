package main

import (
	"context"
	"log"

	"github.com/meshtrade/mesh-etl/etl/pipeline"
)

type Model struct {
	Value int
}

func main() {
	pipeline := pipeline.NewPipeline(
		pipeline.SequenceSource(
			pipeline.SourceScalar(func(ctx context.Context) (int, error) {
				return 0, nil
			}),
			pipeline.ChainedSourceScalar(func(ctx context.Context, in int) ([]int, error) {
				return []int{1, 2, 3, 4}, nil
			}),
		),
		pipeline.SequenceStage(
			pipeline.Map(func(ctx context.Context, inValue int) int {
				return inValue * inValue
			}),
			pipeline.Map(func(ctx context.Context, inValue int) Model {
				return Model{
					Value: inValue,
				}
			}),
		),
		pipeline.Emit(NewSTDOutEmitter[Model]().Emit),
	)

	if err := pipeline.Execute(context.Background()); err != nil {
		log.Fatal(err)
	}
}
