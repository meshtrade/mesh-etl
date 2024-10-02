package examples

import (
	"context"
	"log"

	"github.com/meshtrade/mesh-etl/etl"
	"github.com/meshtrade/mesh-etl/etl/parquet"
)

type Model struct {
	value string
	ascii []rune
}

func main() {
	simplePipeline := etl.NewPipeline(
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
				value: s,
				ascii: asciiValues,
			}
		},

		// serialise
		&parquet.ParquetSerialiser[Model]{},

		// emit
		NewSTDOutEmitter(),
	)

	if err := simplePipeline.Execute(context.Background()); err != nil {
		log.Fatal("error during pipeline execution")
	}
}
