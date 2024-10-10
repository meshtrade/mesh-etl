package pipeline

import "context"

type Empty struct{}

type Node[From, To any] interface {
	Execute(ctx context.Context, f From) (To, error)
}
