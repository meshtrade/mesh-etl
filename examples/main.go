package examples

import (
	"github.com/meshtrade/mesh-etl/etl/parquet"
	"github.com/meshtrade/mesh-etl/etl/pipeline"
)

type Model struct {
	Value string
	Ascii []rune
}

func Main() {

	pipeline.NewPipeline(
		pipeline.Source[string](
			NewSliceDataCollector(
				[]string{},
				0,
			).Collect,
		),
		pipeline.Stage[string, byte](
			parquet.NewParquetSerialiser[string]().Marshal,
		),
		pipeline.Sink[byte](
			NewSTDOutEmitter().Emit,
		),
	)

}
