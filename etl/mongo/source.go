package mongo

import (
	"context"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoCollector[T any] struct {
	collection  mongo.Collection
	query       bson.D
	findOptions []*options.FindOptions
}

func NewMongoCollector[T any](collection mongo.Collection, query bson.D, opts ...*options.FindOptions) *MongoCollector[T] {
	return &MongoCollector[T]{
		collection:  collection,
		findOptions: opts,
	}
}

func (m *MongoCollector[T]) Collect(ctx context.Context) ([]T, error) {
	cursor, err := m.collection.Find(
		ctx,
		m.query,
		m.findOptions...,
	)
	if err != nil {
		return nil, err
	}

	records := []T{}
	if err := cursor.All(ctx, records); err != nil {
		return nil, err
	}

	return records, nil
}
