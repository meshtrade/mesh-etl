package pipeline

type Pipeline[T, V, K any] struct {
	source Source[T]
	stage  Stage[T, V]
	sink   Sink[K]
}

func NewPipeline[T, V, K any](
	source Source[T],
	stage Stage[T, V],
	sink Sink[K],
) *Pipeline[T, V, K] {
	return &Pipeline[T, V, K]{
		source: source,
		stage:  stage,
		sink:   sink,
	}
}
