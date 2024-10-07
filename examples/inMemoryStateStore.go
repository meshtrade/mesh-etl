package examples

import (
	"context"
	"errors"

	"github.com/meshtrade/mesh-etl/etl"
)

var (
	ErrNotFound = errors.New("not found")
)

var _ etl.StateStore = &InMemoryStateStore{}

type InMemoryStateStore struct {
	store map[string]string
}

func NewInMemoryStateStore() *InMemoryStateStore {
	return &InMemoryStateStore{
		store: map[string]string{},
	}
}

func (i *InMemoryStateStore) Get(ctx context.Context, key string) (string, error) {
	value, found := i.store[key]
	if !found {
		return "", ErrNotFound
	}

	return value, nil
}

func (i *InMemoryStateStore) Set(ctx context.Context, key string, value string) error {
	i.store[key] = value
	return nil
}
