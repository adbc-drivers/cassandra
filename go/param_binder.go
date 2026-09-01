// Copyright (c) 2026 ADBC Drivers Contributors
//
// This file has been modified from its original version, which is
// under the Apache License:
//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this file for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package cassandra

import (
	"fmt"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"gopkg.in/inf.v0"
)

type paramBinder interface {
	Bind(record arrow.RecordBatch, rowIdx int) (any, error)
}

func makeParamBinders(schema *arrow.Schema) ([]paramBinder, error) {
	binders := make([]paramBinder, schema.NumFields())
	for i, field := range schema.Fields() {
		binder, err := makeParamBinder(field, i)
		if err != nil {
			return nil, err
		}
		binders[i] = binder
	}
	return binders, nil
}

func fillBindParams(binders []paramBinder, record arrow.RecordBatch, rowIdx int, params []any) error {
	for i, binder := range binders {
		value, err := binder.Bind(record, rowIdx)
		if err != nil {
			return adbc.Error{
				Code: adbc.StatusInvalidData,
				Msg:  fmt.Sprintf("[cassandra] failed to extract bind parameter %d: %v", i, err),
			}
		}
		params[i] = value
	}
	return nil
}

func makeParamBinder(field arrow.Field, colIdx int) (paramBinder, error) {
	switch field.Type.ID() {
	case arrow.NULL:
		return nullBinder{}, nil
	case arrow.BOOL:
		return &boolBinder{colIdx: colIdx}, nil
	case arrow.INT8:
		return &int8Binder{colIdx: colIdx}, nil
	case arrow.INT16:
		return &int16Binder{colIdx: colIdx}, nil
	case arrow.INT32:
		return &int32Binder{colIdx: colIdx}, nil
	case arrow.INT64:
		return &int64Binder{colIdx: colIdx}, nil
	case arrow.UINT8:
		return &uint8Binder{colIdx: colIdx}, nil
	case arrow.UINT16:
		return &uint16Binder{colIdx: colIdx}, nil
	case arrow.UINT32:
		return &uint32Binder{colIdx: colIdx}, nil
	case arrow.UINT64:
		return &uint64Binder{colIdx: colIdx}, nil
	case arrow.FLOAT16:
		return &float16Binder{colIdx: colIdx}, nil
	case arrow.FLOAT32:
		return &float32Binder{colIdx: colIdx}, nil
	case arrow.FLOAT64:
		return &float64Binder{colIdx: colIdx}, nil
	case arrow.STRING:
		return &stringBinder{colIdx: colIdx}, nil
	case arrow.STRING_VIEW:
		return &stringViewBinder{colIdx: colIdx}, nil
	case arrow.LARGE_STRING:
		return &largeStringBinder{colIdx: colIdx}, nil
	case arrow.BINARY:
		return &binaryBinder{colIdx: colIdx}, nil
	case arrow.BINARY_VIEW:
		return &binaryViewBinder{colIdx: colIdx}, nil
	case arrow.LARGE_BINARY:
		return &largeBinaryBinder{colIdx: colIdx}, nil
	case arrow.FIXED_SIZE_BINARY:
		return &fixedBinaryBinder{colIdx: colIdx}, nil
	case arrow.TIMESTAMP:
		return &timestampBinder{colIdx: colIdx, unit: field.Type.(*arrow.TimestampType).Unit}, nil
	case arrow.TIME32:
		return &time32Binder{colIdx: colIdx, unit: field.Type.(*arrow.Time32Type).Unit}, nil
	case arrow.TIME64:
		return &time64Binder{colIdx: colIdx, unit: field.Type.(*arrow.Time64Type).Unit}, nil
	case arrow.DATE32:
		return &date32Binder{colIdx: colIdx}, nil
	case arrow.DATE64:
		return &date64Binder{colIdx: colIdx}, nil
	case arrow.DECIMAL:
		return &decimal128Binder{colIdx: colIdx, scale: field.Type.(*arrow.Decimal128Type).Scale}, nil
	case arrow.DICTIONARY:
		valueField := field
		valueField.Type = field.Type.(*arrow.DictionaryType).ValueType
		if _, err := makeParamBinder(valueField, colIdx); err != nil {
			return nil, err
		}
		return &dictionaryBinder{colIdx: colIdx}, nil
	case arrow.LIST, arrow.FIXED_SIZE_LIST, arrow.MAP:
		// Cassandra list<T>/set<T> bind from an Arrow list column, and map<K,V>
		// from an Arrow map column. A fixed-size list represents a CQL vector.
		return &collectionBinder{colIdx: colIdx}, nil
	default:
		return nil, adbc.Error{
			Code: adbc.StatusNotImplemented,
			Msg:  fmt.Sprintf("[cassandra] unsupported bind parameter type %s for field %s", field.Type, field.Name),
		}
	}
}

type nullBinder struct{}

func (nullBinder) Bind(arrow.RecordBatch, int) (any, error) {
	return nil, nil
}

type dictionaryBinder struct{ colIdx int }

func (b *dictionaryBinder) Bind(record arrow.RecordBatch, rowIdx int) (any, error) {
	col := record.Column(b.colIdx).(*array.Dictionary)
	if col.IsNull(rowIdx) {
		return nil, nil
	}
	return getValueFromColumn(col.Dictionary(), col.GetValueIndex(rowIdx))
}

type boolBinder struct{ colIdx int }

func (b *boolBinder) Bind(record arrow.RecordBatch, rowIdx int) (any, error) {
	col := record.Column(b.colIdx)
	if col.IsNull(rowIdx) {
		return nil, nil
	}
	return col.(*array.Boolean).Value(rowIdx), nil
}

type int8Binder struct{ colIdx int }

func (b *int8Binder) Bind(record arrow.RecordBatch, rowIdx int) (any, error) {
	col := record.Column(b.colIdx)
	if col.IsNull(rowIdx) {
		return nil, nil
	}
	return col.(*array.Int8).Value(rowIdx), nil
}

type int16Binder struct{ colIdx int }

func (b *int16Binder) Bind(record arrow.RecordBatch, rowIdx int) (any, error) {
	col := record.Column(b.colIdx)
	if col.IsNull(rowIdx) {
		return nil, nil
	}
	return col.(*array.Int16).Value(rowIdx), nil
}

type int32Binder struct{ colIdx int }

func (b *int32Binder) Bind(record arrow.RecordBatch, rowIdx int) (any, error) {
	col := record.Column(b.colIdx)
	if col.IsNull(rowIdx) {
		return nil, nil
	}
	return col.(*array.Int32).Value(rowIdx), nil
}

type int64Binder struct{ colIdx int }

func (b *int64Binder) Bind(record arrow.RecordBatch, rowIdx int) (any, error) {
	col := record.Column(b.colIdx)
	if col.IsNull(rowIdx) {
		return nil, nil
	}
	return col.(*array.Int64).Value(rowIdx), nil
}

type uint8Binder struct{ colIdx int }

func (b *uint8Binder) Bind(record arrow.RecordBatch, rowIdx int) (any, error) {
	col := record.Column(b.colIdx)
	if col.IsNull(rowIdx) {
		return nil, nil
	}
	return col.(*array.Uint8).Value(rowIdx), nil
}

type uint16Binder struct{ colIdx int }

func (b *uint16Binder) Bind(record arrow.RecordBatch, rowIdx int) (any, error) {
	col := record.Column(b.colIdx)
	if col.IsNull(rowIdx) {
		return nil, nil
	}
	return col.(*array.Uint16).Value(rowIdx), nil
}

type uint32Binder struct{ colIdx int }

func (b *uint32Binder) Bind(record arrow.RecordBatch, rowIdx int) (any, error) {
	col := record.Column(b.colIdx)
	if col.IsNull(rowIdx) {
		return nil, nil
	}
	return col.(*array.Uint32).Value(rowIdx), nil
}

type uint64Binder struct{ colIdx int }

func (b *uint64Binder) Bind(record arrow.RecordBatch, rowIdx int) (any, error) {
	col := record.Column(b.colIdx)
	if col.IsNull(rowIdx) {
		return nil, nil
	}
	return col.(*array.Uint64).Value(rowIdx), nil
}

type float16Binder struct{ colIdx int }

func (b *float16Binder) Bind(record arrow.RecordBatch, rowIdx int) (any, error) {
	col := record.Column(b.colIdx)
	if col.IsNull(rowIdx) {
		return nil, nil
	}
	return col.(*array.Float16).Value(rowIdx).Float32(), nil
}

type float32Binder struct{ colIdx int }

func (b *float32Binder) Bind(record arrow.RecordBatch, rowIdx int) (any, error) {
	col := record.Column(b.colIdx)
	if col.IsNull(rowIdx) {
		return nil, nil
	}
	return col.(*array.Float32).Value(rowIdx), nil
}

type float64Binder struct{ colIdx int }

func (b *float64Binder) Bind(record arrow.RecordBatch, rowIdx int) (any, error) {
	col := record.Column(b.colIdx)
	if col.IsNull(rowIdx) {
		return nil, nil
	}
	return col.(*array.Float64).Value(rowIdx), nil
}

type stringBinder struct{ colIdx int }

func (b *stringBinder) Bind(record arrow.RecordBatch, rowIdx int) (any, error) {
	col := record.Column(b.colIdx)
	if col.IsNull(rowIdx) {
		return nil, nil
	}
	return col.(*array.String).Value(rowIdx), nil
}

type stringViewBinder struct{ colIdx int }

func (b *stringViewBinder) Bind(record arrow.RecordBatch, rowIdx int) (any, error) {
	col := record.Column(b.colIdx)
	if col.IsNull(rowIdx) {
		return nil, nil
	}
	return col.(*array.StringView).Value(rowIdx), nil
}

type largeStringBinder struct{ colIdx int }

func (b *largeStringBinder) Bind(record arrow.RecordBatch, rowIdx int) (any, error) {
	col := record.Column(b.colIdx)
	if col.IsNull(rowIdx) {
		return nil, nil
	}
	return col.(*array.LargeString).Value(rowIdx), nil
}

type binaryBinder struct{ colIdx int }

func (b *binaryBinder) Bind(record arrow.RecordBatch, rowIdx int) (any, error) {
	col := record.Column(b.colIdx)
	if col.IsNull(rowIdx) {
		return nil, nil
	}
	return col.(*array.Binary).Value(rowIdx), nil
}

type binaryViewBinder struct{ colIdx int }

func (b *binaryViewBinder) Bind(record arrow.RecordBatch, rowIdx int) (any, error) {
	col := record.Column(b.colIdx)
	if col.IsNull(rowIdx) {
		return nil, nil
	}
	return col.(*array.BinaryView).Value(rowIdx), nil
}

type largeBinaryBinder struct{ colIdx int }

func (b *largeBinaryBinder) Bind(record arrow.RecordBatch, rowIdx int) (any, error) {
	col := record.Column(b.colIdx)
	if col.IsNull(rowIdx) {
		return nil, nil
	}
	return col.(*array.LargeBinary).Value(rowIdx), nil
}

type fixedBinaryBinder struct{ colIdx int }

func (b *fixedBinaryBinder) Bind(record arrow.RecordBatch, rowIdx int) (any, error) {
	col := record.Column(b.colIdx)
	if col.IsNull(rowIdx) {
		return nil, nil
	}
	return col.(*array.FixedSizeBinary).Value(rowIdx), nil
}

type timestampBinder struct {
	colIdx int
	unit   arrow.TimeUnit
}

func (b *timestampBinder) Bind(record arrow.RecordBatch, rowIdx int) (any, error) {
	col := record.Column(b.colIdx)
	if col.IsNull(rowIdx) {
		return nil, nil
	}
	return timestampToMillis(int64(col.(*array.Timestamp).Value(rowIdx)), b.unit), nil
}

type time32Binder struct {
	colIdx int
	unit   arrow.TimeUnit
}

func (b *time32Binder) Bind(record arrow.RecordBatch, rowIdx int) (any, error) {
	col := record.Column(b.colIdx)
	if col.IsNull(rowIdx) {
		return nil, nil
	}
	return time32ToNanos(int64(col.(*array.Time32).Value(rowIdx)), b.unit), nil
}

type time64Binder struct {
	colIdx int
	unit   arrow.TimeUnit
}

func (b *time64Binder) Bind(record arrow.RecordBatch, rowIdx int) (any, error) {
	col := record.Column(b.colIdx)
	if col.IsNull(rowIdx) {
		return nil, nil
	}
	return time64ToNanos(int64(col.(*array.Time64).Value(rowIdx)), b.unit), nil
}

type date32Binder struct{ colIdx int }

func (b *date32Binder) Bind(record arrow.RecordBatch, rowIdx int) (any, error) {
	col := record.Column(b.colIdx)
	if col.IsNull(rowIdx) {
		return nil, nil
	}
	return int64(col.(*array.Date32).Value(rowIdx)) * 24 * 60 * 60 * 1000, nil
}

type date64Binder struct{ colIdx int }

func (b *date64Binder) Bind(record arrow.RecordBatch, rowIdx int) (any, error) {
	col := record.Column(b.colIdx)
	if col.IsNull(rowIdx) {
		return nil, nil
	}
	return int64(col.(*array.Date64).Value(rowIdx)), nil
}

type decimal128Binder struct {
	colIdx int
	scale  int32
}

func (b *decimal128Binder) Bind(record arrow.RecordBatch, rowIdx int) (any, error) {
	col := record.Column(b.colIdx)
	if col.IsNull(rowIdx) {
		return nil, nil
	}
	return inf.NewDecBig(col.(*array.Decimal128).Value(rowIdx).BigInt(), inf.Scale(b.scale)), nil
}

// collectionBinder binds Arrow list, fixed-size list, and map columns. gocql
// marshals the Go slice/map that getValueFromColumn produces into a Cassandra
// list, set, map, or vector.
type collectionBinder struct{ colIdx int }

func (b *collectionBinder) Bind(record arrow.RecordBatch, rowIdx int) (any, error) {
	col := record.Column(b.colIdx)
	if col.IsNull(rowIdx) {
		return nil, nil
	}
	return getValueFromColumn(col, rowIdx)
}
