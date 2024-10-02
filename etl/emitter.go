package etl

import "context"

type DataEmitter interface {
	Emit(ctx context.Context, data []byte) error
}
