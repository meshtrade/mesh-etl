package parquet

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/apache/arrow/go/v17/parquet"
	"github.com/apache/arrow/go/v17/parquet/pqarrow"
	"github.com/meshtrade/mesh-etl/etl/pipeline"
	"github.com/rs/zerolog/log"
)

type ParquetSerialiser[T any] struct {
	allocator     memory.Allocator
	schema        *arrow.Schema
	fieldBuilders []array.Builder
}

func NewParquetSerialiser[T any]() *ParquetSerialiser[T] {
	// create new go allocator
	pool := memory.NewGoAllocator()

	// check element type
	var t T
	elemType := reflect.TypeOf(t)
	if elemType.Kind() != reflect.Struct {
		log.Fatal().Msg("expected type for serialiser to be struct")
	}

	// dynamically build the Arrow schema based on the struct fields
	arrowFields := []arrow.Field{}
	fieldBuilders := []array.Builder{}
	arrowFields, fieldBuilders, err := buildArrowFieldsAndBuilders(arrowFields, fieldBuilders, pool, elemType)
	if err != nil {
		log.Fatal().Err(err).Msg("error building arrow fields and builders")
	}

	// build schema from fields of type T
	schema := arrow.NewSchema(arrowFields, nil)

	return &ParquetSerialiser[T]{
		allocator:     pool,
		schema:        schema,
		fieldBuilders: fieldBuilders,
	}
}

// buildArrowFieldsAndBuilders will build arrow fields and arrow field builders for a given reflect.Type
func buildArrowFieldsAndBuilders(arrowFields []arrow.Field, fieldBuilders []array.Builder, pool memory.Allocator, elemType reflect.Type) ([]arrow.Field, []array.Builder, error) {
	for i := 0; i < elemType.NumField(); i++ {
		field := elemType.Field(i)

		switch field.Type.Kind() {
		case reflect.String:
			arrowFields = append(arrowFields, arrow.Field{Name: field.Name, Type: arrow.BinaryTypes.String, Nullable: false})
			fieldBuilders = append(fieldBuilders, array.NewStringBuilder(pool))

		case reflect.Int32:
			arrowFields = append(arrowFields, arrow.Field{Name: field.Name, Type: arrow.PrimitiveTypes.Int32, Nullable: false})
			fieldBuilders = append(fieldBuilders, array.NewInt32Builder(pool))

		case reflect.Float64:
			arrowFields = append(arrowFields, arrow.Field{Name: field.Name, Type: arrow.PrimitiveTypes.Float64, Nullable: false})
			fieldBuilders = append(fieldBuilders, array.NewFloat64Builder(pool))

		case reflect.Struct:
			if field.Type == reflect.TypeOf(time.Time{}) {
				arrowFields = append(arrowFields, arrow.Field{Name: field.Name, Type: arrow.FixedWidthTypes.Date64, Nullable: false})
				fieldBuilders = append(fieldBuilders, array.NewDate64Builder(pool))
				continue
			}

			// recursively build schema for the inner struct
			var err error
			arrowFields, fieldBuilders, err = buildArrowFieldsAndBuilders(arrowFields, fieldBuilders, pool, field.Type)
			if err != nil {
				return nil, nil, err
			}

		default:
			return nil, nil, fmt.Errorf("unsupported field type for field %s: %s", field.Name, field.Type.Kind())
		}
	}

	return arrowFields, fieldBuilders, nil
}

func (s *ParquetSerialiser[T]) Serialise(ctx context.Context, p *pipeline.PipelineState, inChannel chan T) (chan []byte, error) {
	// collect values from channel
	inputStruct := []T{}
	for inValue := range inChannel {
		inputStruct = append(inputStruct, inValue)
	}

	// check if there is data to serialise
	if len(inputStruct) == 0 {
		outChannel := make(chan []byte, 1)
		outChannel <- []byte{}
		close(outChannel)
		return outChannel, nil
	}

	// iterate through the slice and append values to builders
	for i := 0; i < len(inputStruct); i++ {
		structVal := reflect.ValueOf(inputStruct[i])

		// add struct values for given struct
		builderIdx := 0
		s.appendStructValues(builderIdx, structVal)
	}

	// create arrow arrays from builders
	arrowArrays := make([]arrow.Array, len(s.fieldBuilders))
	for i, builder := range s.fieldBuilders {
		arrowArrays[i] = builder.NewArray()
		defer arrowArrays[i].Release()
	}

	// create arrow record
	record := array.NewRecord(s.schema, arrowArrays, int64(len(inputStruct)))

	var dataBuffer bytes.Buffer

	// prepare writer to write data to buffer
	pw, err := pqarrow.NewFileWriter(
		s.schema,
		&dataBuffer,
		parquet.NewWriterProperties(),
		pqarrow.DefaultWriterProps(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create Parquet writer: %v", err)
	}

	// write the record to the bytes buffer
	if err := pw.Write(record); err != nil {
		return nil, fmt.Errorf("failed to write record to Parquet file: %v", err)
	}

	// NOTE: NEVER call close in defer function!
	pw.Close()

	// load value into output channel
	outputChannel := make(chan []byte, 1)
	outputChannel <- dataBuffer.Bytes()
	close(outputChannel)

	return outputChannel, nil
}

func (s *ParquetSerialiser[T]) appendStructValues(builderIdx int, structVal reflect.Value) (int, error) {
	timeType := reflect.TypeOf(time.Time{})

	for j := 0; j < structVal.NumField(); j++ {
		// get a handle to the value of the struct field
		fieldVal := structVal.Field(j)

		switch fieldVal.Kind() {
		case reflect.String:
			// add string value
			s.fieldBuilders[builderIdx].(*array.StringBuilder).Append(fieldVal.String())

		case reflect.Int32:
			// add int value
			s.fieldBuilders[builderIdx].(*array.Int32Builder).Append(int32(fieldVal.Int()))

		case reflect.Float64:
			// add float value
			s.fieldBuilders[builderIdx].(*array.Float64Builder).Append(fieldVal.Float())

		case reflect.Struct:
			if fieldVal.Type() == timeType {
				// add date time value
				timeVal := fieldVal.Interface().(time.Time)
				s.fieldBuilders[builderIdx].(*array.Date64Builder).Append(arrow.Date64FromTime(timeVal))
				continue
			} else {
				// recursively add struct values
				var err error
				builderIdx, err = s.appendStructValues(builderIdx, fieldVal)
				if err != nil {
					return -1, err
				}
			}
		default:
			return -1, fmt.Errorf("unsupported field type: %s", fieldVal.Kind())
		}
		builderIdx++
	}

	return builderIdx, nil
}
