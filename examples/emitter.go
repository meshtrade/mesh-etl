package examples

import "context"

type DataEmitter interface {
	Emit(ctx context.Context, data []byte) error
}
