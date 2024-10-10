package mongo

import (
	"context"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoCollector[T any] struct {
	collection mongo.Collection
	query      bson.D
}

func NewMongoCollector[T any](collection mongo.Collection, query bson.D) *MongoCollector[T] {
	return &MongoCollector[T]{
		collection: collection,
	}
}

func (m *MongoCollector[T]) Collect(ctx context.Context) ([]T, error) {
	cursor, err := m.collection.Find(
		ctx,
		m.query,
		options.Find(),
	)
	if err != nil {
		return nil, err
	}

	results := []T{}
	if err := cursor.All(ctx, results); err != nil {
		return nil, err
	}

	return results, nil
}
