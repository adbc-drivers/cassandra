// Copyright (c) 2026 ADBC Drivers Contributors
//
// This file has been modified from its original version, which is
// under the Apache License:
//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
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
	"context"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"reflect"
	"time"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
	gocql "github.com/apache/cassandra-gocql-driver/v2"
	"github.com/google/uuid"
	"gopkg.in/inf.v0"
)

const queryBatchRowLimit int64 = 1000

type queryRecordReaderImpl struct {
	session      *gocql.Session
	errorHelper  driverbase.ErrorHelper
	query        string
	pageSize     int
	expected     *arrow.Schema
	binders      []paramBinder
	iter         *gocql.Iter
	destinations []any
	getters      []nullableValueGetter
}

var _ driverbase.RecordReaderImpl = (*queryRecordReaderImpl)(nil)

func newQueryRecordReader(ctx context.Context, session *gocql.Session, query string, pageSize int, params array.RecordReader, alloc memory.Allocator, logger *slog.Logger, errorHelper driverbase.ErrorHelper) (array.RecordReader, error) {
	rr := &driverbase.BaseRecordReader{}
	options := driverbase.BaseRecordReaderOptions{BatchRowLimit: queryBatchRowLimit}

	var binders []paramBinder
	var err error
	if params != nil {
		binders, err = makeParamBinders(params.Schema())
		if err != nil {
			return nil, err
		}
	}

	err = rr.Init(ctx, alloc, logger, params, options, &queryRecordReaderImpl{
		session:     session,
		errorHelper: errorHelper,
		query:       query,
		pageSize:    pageSize,
		binders:     binders,
	})
	if err != nil {
		rr.Release()
		return nil, err
	}
	return rr, nil
}

func (r *queryRecordReaderImpl) NextResultSet(ctx context.Context, rec arrow.RecordBatch, rowIdx int) (*arrow.Schema, error) {
	if r.iter != nil {
		if err := r.iter.Close(); err != nil {
			return nil, r.errorHelper.WrapIO(err, "failed to close query iterator")
		}
		r.iter = nil
	}

	var args []any
	if rec != nil {
		args = make([]any, len(r.binders))
		if err := fillBindParams(r.binders, rec, rowIdx, args); err != nil {
			return nil, err
		}
	}

	iter := r.session.Query(r.query, args...).PageSize(r.pageSize).IterContext(ctx)
	schema := schemaFromColumns(iter.Columns())
	if r.expected == nil {
		r.expected = schema
	} else if !r.expected.Equal(schema) {
		if err := iter.Close(); err != nil {
			return nil, r.errorHelper.WrapIO(err, "failed to close query iterator")
		}
		return nil, adbc.Error{
			Code: adbc.StatusInvalidData,
			Msg:  "[cassandra] inconsistent result schema across bound parameter rows",
		}
	}

	destinations, getters, err := makeNullableScanTargets(iter.Columns())
	if err != nil {
		if closeErr := iter.Close(); closeErr != nil {
			return nil, r.errorHelper.WrapIO(closeErr, "failed to close query iterator")
		}
		return nil, err
	}

	r.iter = iter
	r.destinations = destinations
	r.getters = getters
	return schema, nil
}

func (r *queryRecordReaderImpl) BeginAppending(builder *array.RecordBuilder) error {
	return nil
}

func (r *queryRecordReaderImpl) AppendRows(builder *array.RecordBuilder) (int64, int64, error) {
	if r.iter == nil {
		return 0, 0, io.EOF
	}

	if !r.iter.Scan(r.destinations...) {
		err := r.iter.Close()
		r.iter = nil
		if err != nil {
			return 0, 0, r.errorHelper.WrapIO(err, "query failed")
		}
		return 0, 0, io.EOF
	}

	for i, getValue := range r.getters {
		value, isNull, err := getValue()
		if err != nil {
			return 0, 0, adbc.Error{
				Code: adbc.StatusIO,
				Msg:  "[cassandra] failed to decode query result: " + err.Error(),
			}
		}
		if isNull {
			builder.Field(i).AppendNull()
			continue
		}
		if err := appendValue(builder.Field(i), value, builder.Schema().Field(i).Type); err != nil {
			return 0, 0, adbc.Error{
				Code: adbc.StatusIO,
				Msg:  "[cassandra] failed to append query result: " + err.Error(),
			}
		}
	}

	return 1, 0, nil
}

func (r *queryRecordReaderImpl) Close() error {
	if r.iter != nil {
		err := r.iter.Close()
		r.iter = nil
		if err != nil {
			return r.errorHelper.WrapIO(err, "failed to close query iterator")
		}
	}
	return nil
}

func appendValue(builder array.Builder, value any, dataType arrow.DataType) error {
	if value == nil {
		builder.AppendNull()
		return nil
	}

	switch b := builder.(type) {
	case *array.BooleanBuilder:
		if v, ok := value.(bool); ok {
			b.Append(v)
		} else {
			return fmt.Errorf("expected bool, got %T", value)
		}
	case *array.Int8Builder:
		if v, ok := value.(int8); ok {
			b.Append(v)
		} else {
			return fmt.Errorf("expected int8, got %T", value)
		}
	case *array.Int16Builder:
		if v, ok := value.(int16); ok {
			b.Append(v)
		} else {
			return fmt.Errorf("expected int16, got %T", value)
		}
	case *array.Int32Builder:
		if v, ok := value.(int32); ok {
			b.Append(v)
		} else if v, ok := value.(int); ok {
			b.Append(int32(v))
		} else {
			return fmt.Errorf("expected int32, got %T", value)
		}
	case *array.Int64Builder:
		if v, ok := value.(int64); ok {
			b.Append(v)
		} else if v, ok := value.(int); ok {
			b.Append(int64(v))
		} else if v, ok := value.(*big.Int); ok {
			// varint elements inside collections are scanned as *big.Int.
			b.Append(v.Int64())
		} else {
			return fmt.Errorf("expected int64, got %T", value)
		}
	case *array.Float32Builder:
		if v, ok := value.(float32); ok {
			b.Append(v)
		} else {
			return fmt.Errorf("expected float32, got %T", value)
		}
	case *array.Float64Builder:
		if v, ok := value.(float64); ok {
			b.Append(v)
		} else {
			return fmt.Errorf("expected float64, got %T", value)
		}
	case *array.StringBuilder:
		switch v := value.(type) {
		case string:
			b.Append(v)
		case uuid.UUID:
			b.Append(v.String())
		case gocql.UUID:
			b.Append(v.String())
		default:
			b.Append(fmt.Sprintf("%v", value))
		}
	case *array.BinaryBuilder:
		if v, ok := value.([]byte); ok {
			b.Append(v)
		} else {
			return fmt.Errorf("expected []byte, got %T", value)
		}
	case *array.TimestampBuilder:
		switch v := value.(type) {
		case time.Time:
			tsType := dataType.(*arrow.TimestampType)
			ts, err := arrow.TimestampFromTime(v, tsType.Unit)
			if err != nil {
				return fmt.Errorf("failed to convert time to timestamp: %w", err)
			}
			b.Append(ts)
		case int64:
			b.Append(arrow.Timestamp(v))
		default:
			return fmt.Errorf("expected time.Time or int64, got %T", value)
		}
	case *array.Date32Builder:
		switch v := value.(type) {
		case time.Time:
			days := int32(v.Unix() / 86400)
			b.Append(arrow.Date32(days))
		case int32:
			b.Append(arrow.Date32(v))
		default:
			return fmt.Errorf("expected time.Time or int32, got %T", value)
		}
	case *array.Date64Builder:
		switch v := value.(type) {
		case time.Time:
			b.Append(arrow.Date64(v.UnixMilli()))
		case int64:
			b.Append(arrow.Date64(v))
		default:
			return fmt.Errorf("expected time.Time or int64, got %T", value)
		}
	case *array.Time64Builder:
		switch v := value.(type) {
		case time.Duration:
			// Cassandra TIME is stored as nanoseconds since midnight.
			b.Append(arrow.Time64(v.Nanoseconds()))
		case int64:
			// Already in nanoseconds.
			b.Append(arrow.Time64(v))
		default:
			return fmt.Errorf("expected time.Duration or int64, got %T", value)
		}
	case *array.Decimal128Builder:
		switch v := value.(type) {
		case *inf.Dec:
			decimalType, ok := dataType.(*arrow.Decimal128Type)
			if !ok {
				return fmt.Errorf("expected decimal128 data type, got %T", dataType)
			}
			rescaled := new(inf.Dec).Round(v, inf.Scale(decimalType.Scale), inf.RoundExact)
			if rescaled == nil {
				return fmt.Errorf("decimal %s cannot be represented exactly at scale %d", v, decimalType.Scale)
			}
			num := decimal128.FromBigInt(rescaled.UnscaledBig())
			if !num.FitsInPrecision(decimalType.Precision) {
				return fmt.Errorf("decimal %s exceeds precision %d", v, decimalType.Precision)
			}
			b.Append(num)
		default:
			return fmt.Errorf("expected *inf.Dec for decimal, got %T", value)
		}
	case *array.ListBuilder:
		// Cassandra list<T>/set<T> scanned as a Go slice (e.g. []string).
		rv := reflect.ValueOf(value)
		if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
			return fmt.Errorf("expected slice for list, got %T", value)
		}
		listType, ok := dataType.(*arrow.ListType)
		if !ok {
			return fmt.Errorf("expected list data type, got %T", dataType)
		}
		b.Append(true)
		elemType := listType.Elem()
		valueBuilder := b.ValueBuilder()
		for i := range rv.Len() {
			if err := appendValue(valueBuilder, rv.Index(i).Interface(), elemType); err != nil {
				return fmt.Errorf("list element %d: %w", i, err)
			}
		}
	case *array.FixedSizeListBuilder:
		// Cassandra vector<T, N> scanned as a fixed-size Go slice.
		rv := reflect.ValueOf(value)
		if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
			return fmt.Errorf("expected slice for fixed-size list, got %T", value)
		}
		listType, ok := dataType.(*arrow.FixedSizeListType)
		if !ok {
			return fmt.Errorf("expected fixed-size list data type, got %T", dataType)
		}
		if rv.Len() != int(listType.Len()) {
			return fmt.Errorf("expected fixed-size list of length %d, got %d", listType.Len(), rv.Len())
		}
		b.Append(true)
		elemType := listType.Elem()
		valueBuilder := b.ValueBuilder()
		for i := range rv.Len() {
			if err := appendValue(valueBuilder, rv.Index(i).Interface(), elemType); err != nil {
				return fmt.Errorf("fixed-size list element %d: %w", i, err)
			}
		}
	case *array.MapBuilder:
		// Cassandra map<K,V> scanned as a Go map (e.g. map[string]string).
		rv := reflect.ValueOf(value)
		if rv.Kind() != reflect.Map {
			return fmt.Errorf("expected map for map, got %T", value)
		}
		mapType, ok := dataType.(*arrow.MapType)
		if !ok {
			return fmt.Errorf("expected map data type, got %T", dataType)
		}
		b.Append(true)
		keyType := mapType.KeyType()
		itemType := mapType.ItemType()
		keyBuilder := b.KeyBuilder()
		itemBuilder := b.ItemBuilder()
		iter := rv.MapRange()
		for iter.Next() {
			if err := appendValue(keyBuilder, iter.Key().Interface(), keyType); err != nil {
				return fmt.Errorf("map key: %w", err)
			}
			if err := appendValue(itemBuilder, iter.Value().Interface(), itemType); err != nil {
				return fmt.Errorf("map value: %w", err)
			}
		}
	default:
		return fmt.Errorf("unsupported builder type: %T", builder)
	}

	return nil
}

type nullableValueGetter func() (value any, isNull bool, err error)

// asVectorType identifies gocql's vector TypeInfo. VectorType reports itself
// as TypeCustom, so the collection type switches below cannot identify it by
// TypeInfo.Type() alone.
func asVectorType(info gocql.TypeInfo) (gocql.VectorType, bool) {
	switch info := info.(type) {
	case gocql.VectorType:
		return info, true
	case *gocql.VectorType:
		return *info, true
	default:
		return gocql.VectorType{}, false
	}
}

func makeNullableScanTargets(columns []gocql.ColumnInfo) ([]any, []nullableValueGetter, error) {
	destinations := make([]any, len(columns))
	getters := make([]nullableValueGetter, len(columns))

	for i, column := range columns {
		// gocql only preserves NULL vs zero values when scanning into **T-style destinations.
		destination, getter, err := makeNullableScanTarget(column.TypeInfo)
		if err != nil {
			return nil, nil, fmt.Errorf("column %s: %w", column.Name, err)
		}
		destinations[i] = destination
		getters[i] = getter
	}

	return destinations, getters, nil
}

func makeNullableScanTarget(typeInfo gocql.TypeInfo) (any, nullableValueGetter, error) {
	if _, ok := asVectorType(typeInfo); ok {
		zero := typeInfo.Zero()
		ptr := reflect.New(reflect.TypeOf(zero))
		return ptr.Interface(), func() (any, bool, error) {
			elem := ptr.Elem()
			if elem.IsNil() {
				return nil, true, nil
			}
			return elem.Interface(), false, nil
		}, nil
	}

	switch typeInfo.Type() {
	case gocql.TypeAscii, gocql.TypeVarchar, gocql.TypeText, gocql.TypeInet, gocql.TypeUUID, gocql.TypeTimeUUID:
		var value *string
		return &value, func() (any, bool, error) {
			if value == nil {
				return nil, true, nil
			}
			return *value, false, nil
		}, nil
	case gocql.TypeInt:
		var value *int32
		return &value, func() (any, bool, error) {
			if value == nil {
				return nil, true, nil
			}
			return *value, false, nil
		}, nil
	case gocql.TypeBigInt, gocql.TypeCounter, gocql.TypeVarint:
		var value *int64
		return &value, func() (any, bool, error) {
			if value == nil {
				return nil, true, nil
			}
			return *value, false, nil
		}, nil
	case gocql.TypeSmallInt:
		var value *int16
		return &value, func() (any, bool, error) {
			if value == nil {
				return nil, true, nil
			}
			return *value, false, nil
		}, nil
	case gocql.TypeTinyInt:
		var value *int8
		return &value, func() (any, bool, error) {
			if value == nil {
				return nil, true, nil
			}
			return *value, false, nil
		}, nil
	case gocql.TypeFloat:
		var value *float32
		return &value, func() (any, bool, error) {
			if value == nil {
				return nil, true, nil
			}
			return *value, false, nil
		}, nil
	case gocql.TypeDouble:
		var value *float64
		return &value, func() (any, bool, error) {
			if value == nil {
				return nil, true, nil
			}
			return *value, false, nil
		}, nil
	case gocql.TypeBoolean:
		var value *bool
		return &value, func() (any, bool, error) {
			if value == nil {
				return nil, true, nil
			}
			return *value, false, nil
		}, nil
	case gocql.TypeTimestamp:
		var value *time.Time
		return &value, func() (any, bool, error) {
			if value == nil {
				return nil, true, nil
			}
			return *value, false, nil
		}, nil
	case gocql.TypeDate:
		var value *time.Time
		return &value, func() (any, bool, error) {
			if value == nil {
				return nil, true, nil
			}
			return *value, false, nil
		}, nil
	case gocql.TypeTime:
		var value *time.Duration
		return &value, func() (any, bool, error) {
			if value == nil {
				return nil, true, nil
			}
			return *value, false, nil
		}, nil
	case gocql.TypeBlob:
		var value *[]byte
		return &value, func() (any, bool, error) {
			if value == nil {
				return nil, true, nil
			}
			copied := append([]byte(nil), (*value)...)
			return copied, false, nil
		}, nil
	case gocql.TypeDecimal:
		var value *inf.Dec
		return &value, func() (any, bool, error) {
			if value == nil {
				return nil, true, nil
			}
			return value, false, nil
		}, nil
	case gocql.TypeList, gocql.TypeSet, gocql.TypeMap:
		// Scan into a pointer to the concrete Go slice/map type that gocql uses
		// for this collection (e.g. *[]string, *map[string]int32), obtained from
		// TypeInfo.Zero(). Scanning into a bare *interface{} instead would panic
		// inside gocql when a NULL collection is the first row scanned into a
		// still-empty interface (see gocql marshal.go Unmarshal, interface case).
		zero := typeInfo.Zero()
		ptr := reflect.New(reflect.TypeOf(zero))
		return ptr.Interface(), func() (any, bool, error) {
			// Cassandra stores an empty collection as NULL and returns no value
			// for a NULL column, so gocql leaves the destination as a typed nil
			// slice/map (e.g. []string(nil)); report that as a NULL column.
			elem := ptr.Elem()
			if elem.IsNil() {
				return nil, true, nil
			}
			return elem.Interface(), false, nil
		}, nil
	default:
		var value *string
		return &value, func() (any, bool, error) {
			if value == nil {
				return nil, true, nil
			}
			return *value, false, nil
		}, nil
	}
}

func gocqlTypeToArrow(typeInfo gocql.TypeInfo) arrow.DataType {
	if vectorType, ok := asVectorType(typeInfo); ok {
		return arrow.FixedSizeListOf(int32(vectorType.Dimensions), gocqlTypeToArrow(vectorType.SubType))
	}

	switch typeInfo.Type() {
	case gocql.TypeAscii, gocql.TypeVarchar, gocql.TypeText:
		return arrow.BinaryTypes.String
	case gocql.TypeInt:
		return arrow.PrimitiveTypes.Int32
	case gocql.TypeBigInt, gocql.TypeCounter:
		return arrow.PrimitiveTypes.Int64
	case gocql.TypeSmallInt:
		return arrow.PrimitiveTypes.Int16
	case gocql.TypeTinyInt:
		return arrow.PrimitiveTypes.Int8
	case gocql.TypeFloat:
		return arrow.PrimitiveTypes.Float32
	case gocql.TypeDouble:
		return arrow.PrimitiveTypes.Float64
	case gocql.TypeBoolean:
		return arrow.FixedWidthTypes.Boolean
	case gocql.TypeTimestamp:
		return &arrow.TimestampType{Unit: arrow.Millisecond}
	case gocql.TypeDate:
		return arrow.FixedWidthTypes.Date32
	case gocql.TypeTime:
		return &arrow.Time64Type{Unit: arrow.Nanosecond}
	case gocql.TypeUUID, gocql.TypeTimeUUID:
		return arrow.BinaryTypes.String
	case gocql.TypeBlob:
		return arrow.BinaryTypes.Binary
	case gocql.TypeDecimal:
		return defaultDecimalType()
	case gocql.TypeVarint:
		return arrow.PrimitiveTypes.Int64
	case gocql.TypeInet:
		return arrow.BinaryTypes.String
	case gocql.TypeList, gocql.TypeSet:
		// list<T> and set<T> both map to an Arrow list. Cassandra has no set
		// type in Arrow, and a set carries no ordering/uniqueness guarantees we
		// can express, so it is represented as a list like list<T>.
		if ct, ok := typeInfo.(gocql.CollectionType); ok {
			return arrow.ListOf(gocqlTypeToArrow(ct.Elem))
		}
		return arrow.BinaryTypes.String
	case gocql.TypeMap:
		if ct, ok := typeInfo.(gocql.CollectionType); ok {
			return arrow.MapOf(gocqlTypeToArrow(ct.Key), gocqlTypeToArrow(ct.Elem))
		}
		return arrow.BinaryTypes.String
	case gocql.TypeTuple, gocql.TypeUDT:
		// Tuples and UDTs are not yet mapped to Arrow struct types; fall back to
		// a string representation.
		return arrow.BinaryTypes.String
	default:
		return arrow.BinaryTypes.String
	}
}
