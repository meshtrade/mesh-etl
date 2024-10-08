package examples

import (
	"context"
	"log"

	"github.com/meshtrade/mesh-etl/etl"
	"github.com/meshtrade/mesh-etl/etl/parquet"
)

type Model struct {
	Value string
	Ascii []rune
}

func Main() {
	stateStore := NewInMemoryStateStore()

	stateStore.Set(context.Background(), "idx", "0")

	simplePipeline := etl.NewPipeline(
		stateStore,
		// collect
		NewSliceDataCollector(
			[]string{"a", "b", "c", "d", "e"},
			1,
		),

		// translate
		func(s string) Model {
			asciiValues := []rune{}
			for _, v := range s {
				asciiValues = append(asciiValues, v)
			}
			return Model{
				Value: s,
				Ascii: asciiValues,
			}
		},

		// serialise
		&parquet.ParquetSerialiser[Model]{},

		// emit
		NewSTDOutEmitter(),
	)

	if err := simplePipeline.Execute(context.Background(), "idx"); err != nil {
		log.Fatal("error during pipeline execution")
	}
}
