package etl

import "context"

type DataTranslator[T any, V any] interface {
	Translate(ctx context.Context, t T) (V, error)
}
