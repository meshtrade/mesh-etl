package etl

type DataTranslator[T any, V any] func(T) V
