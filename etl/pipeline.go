package etl

import (
	"context"
	"fmt"

	"github.com/meshtrade/mesh-etl/etl/parquet"
	"github.com/rs/zerolog/log"
)

type Pipeline[T any, V any] struct {
	DataCollector     DataCollector[T]
	DataTranslator    DataTranslator[T, V]
	ParquetSerialiser *parquet.ParquetSerialiser[V]
	DataEmitter       DataEmitter
}

func NewPipeline[T any, V any](
	DataCollector DataCollector[T],
	DataTranslator DataTranslator[T, V],
	ParquetSerialiser *parquet.ParquetSerialiser[V],
	DataEmitter DataEmitter,
) *Pipeline[T, V] {
	return &Pipeline[T, V]{
		DataCollector:     DataCollector,
		DataTranslator:    DataTranslator,
		ParquetSerialiser: ParquetSerialiser,
		DataEmitter:       DataEmitter,
	}
}

func (p *Pipeline[T, V]) Execute(ctx context.Context) error {
	// collect data
	records, _, err := p.DataCollector.Collect(ctx, "")
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("error collecting data")
		return fmt.Errorf("error collecting data: %w", err)
	}

	// translate data (sequentially)
	recordsPrime := make([]V, len(records))
	for idx, record := range records {
		recordsPrime[idx] = p.DataTranslator(record)
	}

	// marshal translated data
	data, err := p.ParquetSerialiser.Marshal(recordsPrime)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("error marshalling model records")
		return fmt.Errorf("error marshalling model records: %w", err)
	}

	// emit marshalled data
	if err := p.DataEmitter.Emit(ctx, data); err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("error emitting data")
		return fmt.Errorf("error emitting data: %w", err)
	}

	return nil
}
