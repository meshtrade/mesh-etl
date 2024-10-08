package etl

import (
	"context"
	"fmt"

	"github.com/meshtrade/mesh-etl/etl/parquet"
)

type Pipeline[T any, V any] struct {
	stateStore        StateStore
	dataCollector     DataCollector[T]
	dataTranslator    DataTranslator[T, V]
	parquetSerialiser *parquet.ParquetSerialiser[V]
	dataEmitter       DataEmitter
}

func NewPipeline[T any, V any](
	stateStore StateStore,
	dataCollector DataCollector[T],
	dataTranslator DataTranslator[T, V],
	parquetSerialiser *parquet.ParquetSerialiser[V],
	dataEmitter DataEmitter,
) *Pipeline[T, V] {
	return &Pipeline[T, V]{
		stateStore:        stateStore,
		dataCollector:     dataCollector,
		dataTranslator:    dataTranslator,
		parquetSerialiser: parquetSerialiser,
		dataEmitter:       dataEmitter,
	}
}

func (p *Pipeline[T, V]) Execute(ctx context.Context, pagingTokenKey string) error {
	// get state
	oldPagingToken, err := p.stateStore.Get(
		ctx,
		pagingTokenKey,
	)
	if err != nil {
		return err
	}

	// collect data
	records, newPagingToken, err := p.dataCollector.Collect(ctx, oldPagingToken)
	if err != nil {
		return fmt.Errorf("error collecting data: %w", err)
	}

	// translate data (sequentially)
	recordsPrime := make([]V, len(records))
	for idx, record := range records {
		recordsPrime[idx] = p.dataTranslator(record)
	}

	// marshal translated data
	data, err := p.parquetSerialiser.Marshal(recordsPrime)
	if err != nil {
		return fmt.Errorf("error marshalling model records: %w", err)
	}

	// emit marshalled data
	if err := p.dataEmitter.Emit(ctx, data); err != nil {
		return fmt.Errorf("error emitting data: %w", err)
	}

	// set paging token in state store
	if err := p.stateStore.Set(
		ctx,
		pagingTokenKey,
		newPagingToken,
	); err != nil {
		return err
	}

	return nil
}
