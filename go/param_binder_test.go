// Copyright (c) 2026 ADBC Drivers Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//         http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cassandra

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFillBindParamsConvertsTemporalUnits(t *testing.T) {
	pool := memory.DefaultAllocator

	timestamps := array.NewTimestampBuilder(pool, &arrow.TimestampType{Unit: arrow.Microsecond})
	defer timestamps.Release()
	timestamps.Append(arrow.Timestamp(1_234_567))

	times := array.NewTime64Builder(pool, &arrow.Time64Type{Unit: arrow.Microsecond})
	defer times.Release()
	times.Append(arrow.Time64(8_765))

	record := array.NewRecordBatch(
		arrow.NewSchema([]arrow.Field{
			{Name: "ts", Type: &arrow.TimestampType{Unit: arrow.Microsecond}},
			{Name: "tm", Type: &arrow.Time64Type{Unit: arrow.Microsecond}},
		}, nil),
		[]arrow.Array{timestamps.NewArray(), times.NewArray()},
		1,
	)
	defer record.Release()

	binders, err := makeParamBinders(record.Schema())
	require.NoError(t, err)

	params := make([]any, len(binders))
	require.NoError(t, fillBindParams(binders, record, 0, params))
	assert.Equal(t, int64(1234), params[0])
	assert.Equal(t, int64(8_765_000), params[1])
}

func TestFillBindParamsDecodesDictionaryValues(t *testing.T) {
	pool := memory.DefaultAllocator

	indicesBuilder := array.NewInt32Builder(pool)
	defer indicesBuilder.Release()
	indicesBuilder.AppendValues([]int32{1, 0, 0}, []bool{true, true, false})
	indices := indicesBuilder.NewArray()
	defer indices.Release()

	intValuesBuilder := array.NewInt32Builder(pool)
	defer intValuesBuilder.Release()
	intValuesBuilder.AppendValues([]int32{10, 20}, nil)
	intValues := intValuesBuilder.NewArray()
	defer intValues.Release()

	stringValuesBuilder := array.NewLargeStringBuilder(pool)
	defer stringValuesBuilder.Release()
	stringValuesBuilder.AppendValues([]string{"alpha", "beta"}, nil)
	stringValues := stringValuesBuilder.NewArray()
	defer stringValues.Release()

	intDictionaryType := &arrow.DictionaryType{
		IndexType: arrow.PrimitiveTypes.Int32,
		ValueType: arrow.PrimitiveTypes.Int32,
	}
	stringDictionaryType := &arrow.DictionaryType{
		IndexType: arrow.PrimitiveTypes.Int32,
		ValueType: arrow.BinaryTypes.LargeString,
	}
	intDictionary := array.NewDictionaryArray(intDictionaryType, indices, intValues)
	defer intDictionary.Release()
	stringDictionary := array.NewDictionaryArray(stringDictionaryType, indices, stringValues)
	defer stringDictionary.Release()

	record := array.NewRecordBatch(
		arrow.NewSchema([]arrow.Field{
			{Name: "idx", Type: intDictionaryType},
			{Name: "res", Type: stringDictionaryType, Nullable: true},
		}, nil),
		[]arrow.Array{intDictionary, stringDictionary},
		3,
	)
	defer record.Release()

	binders, err := makeParamBinders(record.Schema())
	require.NoError(t, err)

	params := make([]any, len(binders))
	require.NoError(t, fillBindParams(binders, record, 0, params))
	assert.Equal(t, []any{int32(20), "beta"}, params)

	require.NoError(t, fillBindParams(binders, record, 1, params))
	assert.Equal(t, []any{int32(10), "alpha"}, params)

	require.NoError(t, fillBindParams(binders, record, 2, params))
	assert.Equal(t, []any{nil, nil}, params)
}

func TestFillBindParamsAcceptsNullType(t *testing.T) {
	nulls := array.NewNull(1)
	defer nulls.Release()

	record := array.NewRecordBatch(
		arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.Null}}, nil),
		[]arrow.Array{nulls},
		1,
	)
	defer record.Release()

	binders, err := makeParamBinders(record.Schema())
	require.NoError(t, err)

	params := make([]any, len(binders))
	require.NoError(t, fillBindParams(binders, record, 0, params))
	assert.Equal(t, []any{nil}, params)
}
