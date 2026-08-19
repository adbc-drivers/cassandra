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
	"strings"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	gocql "github.com/apache/cassandra-gocql-driver/v2"
	"gopkg.in/inf.v0"
)

type statementImpl struct {
	driverbase.StatementImplBase
	connectionImpl *connectionImpl
	query          string
	pageSize       int
	ingest         struct {
		targetTable string
		mode        string
	}
	params array.RecordReader
	closed bool
}

func (s *statementImpl) Base() *driverbase.StatementImplBase {
	return &s.StatementImplBase
}

func (s *statementImpl) checkNotClosed() error {
	if s.closed {
		return adbc.Error{
			Code: adbc.StatusInvalidState,
			Msg:  "[cassandra] statement already closed",
		}
	}
	return nil
}

func (s *statementImpl) Close(ctx context.Context) error {
	if s.closed {
		return adbc.Error{
			Code: adbc.StatusInvalidState,
			Msg:  "[cassandra] statement already closed",
		}
	}
	s.closed = true

	if s.params != nil {
		s.params.Release()
		s.params = nil
	}
	return nil
}

func (s *statementImpl) SetOption(ctx context.Context, key string, val string) error {
	if err := s.checkNotClosed(); err != nil {
		return err
	}
	switch key {
	case adbc.OptionKeyIngestTargetTable:
		s.ingest.targetTable = val
		return nil
	case adbc.OptionKeyIngestMode:
		// Validate ingest mode
		switch val {
		case adbc.OptionValueIngestModeAppend,
			adbc.OptionValueIngestModeCreate,
			adbc.OptionValueIngestModeReplace,
			adbc.OptionValueIngestModeCreateAppend:
			s.ingest.mode = val
			return nil
		default:
			return adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  fmt.Sprintf("[cassandra] invalid ingest mode: %s", val),
			}
		}
	default:
		return adbc.Error{
			Code: adbc.StatusNotImplemented,
			Msg:  fmt.Sprintf("[cassandra] unknown statement option: %s", key),
		}
	}
}

func (s *statementImpl) SetSqlQuery(ctx context.Context, query string) error {
	if err := s.checkNotClosed(); err != nil {
		return err
	}
	s.query = query
	// Clear existing bound state when setting a new query.
	if s.params != nil {
		s.params.Release()
		s.params = nil
	}
	return nil
}

func (s *statementImpl) ExecuteQuery(ctx context.Context) (array.RecordReader, int64, error) {
	if err := s.checkNotClosed(); err != nil {
		return nil, -1, err
	}
	if s.params != nil && s.ingest.targetTable != "" {
		stream := s.params
		s.params = nil
		defer stream.Release()
		rowsAffected, err := s.ExecuteIngest(ctx, stream)
		return nil, rowsAffected, err
	}
	if s.ingest.targetTable == "" && s.params != nil {
		return s.executeBoundQuery(ctx)
	}
	if s.query == "" {
		return nil, -1, adbc.Error{
			Code: adbc.StatusInvalidState,
			Msg:  "[cassandra] no query set",
		}
	}

	reader, err := newQueryRecordReader(ctx, s.connectionImpl.session, s.query, s.pageSize, nil, s.connectionImpl.Alloc, s.connectionImpl.Logger, s.ErrorHelper)
	if err != nil {
		return nil, -1, s.ErrorHelper.WrapIO(err, "failed to create record reader")
	}

	return reader, -1, nil
}

func (s *statementImpl) ExecuteUpdate(ctx context.Context) (int64, error) {
	if err := s.checkNotClosed(); err != nil {
		return -1, err
	}
	if s.params != nil && s.ingest.targetTable != "" {
		stream := s.params
		s.params = nil
		defer stream.Release()
		return s.ExecuteIngest(ctx, stream)
	}
	if s.ingest.targetTable == "" && s.params != nil {
		return s.executeBoundUpdate(ctx)
	}

	if s.query == "" {
		return -1, adbc.Error{
			Code: adbc.StatusInvalidState,
			Msg:  "[cassandra] no query set",
		}
	}

	err := s.connectionImpl.session.Query(s.query).ExecContext(ctx)
	if err != nil {
		return -1, s.ErrorHelper.WrapIO(err, "failed to execute update")
	}

	// Cassandra doesn't return rows affected, return -1
	return -1, nil
}

func (s *statementImpl) Prepare(ctx context.Context) error {
	if err := s.checkNotClosed(); err != nil {
		return err
	}
	// gocql doesn't have an explicit prepare step that we need to call
	// Prepared statements are handled automatically
	return nil
}

func (s *statementImpl) SetSubstraitPlan(ctx context.Context, plan []byte) error {
	if err := s.checkNotClosed(); err != nil {
		return err
	}
	return adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "[cassandra] Substrait plans are not supported",
	}
}

func (s *statementImpl) Bind(ctx context.Context, values arrow.RecordBatch) error {
	if err := s.checkNotClosed(); err != nil {
		return err
	}
	if s.params != nil {
		s.params.Release()
		s.params = nil
	}

	stream, err := array.NewRecordReader(values.Schema(), []arrow.RecordBatch{values})
	if err != nil {
		return adbc.Error{
			Code: adbc.StatusInternal,
			Msg:  fmt.Sprintf("[cassandra] failed to create record reader: %v", err),
		}
	}
	s.params = stream

	// If this is for ingestion (target table set), we're done
	if s.ingest.targetTable != "" {
		return nil
	}

	// Otherwise, this is for query parameter binding
	// Extract parameters from the first row of the record
	if values.NumRows() == 0 {
		return adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  "[cassandra] bind record must have at least one row for parameter binding",
		}
	}

	return nil
}

func (s *statementImpl) BindStream(ctx context.Context, stream array.RecordReader) error {
	if err := s.checkNotClosed(); err != nil {
		return err
	}
	if s.params != nil {
		s.params.Release()
		s.params = nil
	}
	stream.Retain()
	s.params = stream
	return nil
}

func (s *statementImpl) GetParameterSchema(ctx context.Context) (*arrow.Schema, error) {
	if err := s.checkNotClosed(); err != nil {
		return nil, err
	}
	return nil, adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "[cassandra] GetParameterSchema is not implemented",
	}
}

func (s *statementImpl) ExecutePartitions(ctx context.Context) (*arrow.Schema, adbc.Partitions, int64, error) {
	if err := s.checkNotClosed(); err != nil {
		return nil, adbc.Partitions{}, -1, err
	}
	return nil, adbc.Partitions{}, -1, adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "[cassandra] ExecutePartitions is not implemented",
	}
}

func (s *statementImpl) ExecuteSchema(ctx context.Context) (*arrow.Schema, error) {
	if err := s.checkNotClosed(); err != nil {
		return nil, err
	}
	if s.query == "" {
		return nil, adbc.Error{
			Code: adbc.StatusInvalidState,
			Msg:  "[cassandra] no query set",
		}
	}
	if s.params != nil {
		return nil, adbc.Error{
			Code: adbc.StatusNotImplemented,
			Msg:  "[cassandra] ExecuteSchema with bound parameters is not implemented",
		}
	}

	// Execute the query with LIMIT 0 to get schema without data
	// Cassandra doesn't support LIMIT 0 well, so we'll use LIMIT 1 and not fetch data
	limitQuery := s.query

	// Add LIMIT 1 if not already present (simple check)
	if !strings.Contains(strings.ToUpper(limitQuery), "LIMIT") {
		limitQuery += " LIMIT 1"
	}

	// Execute the query to get column metadata
	iter := s.connectionImpl.session.Query(limitQuery).IterContext(ctx)

	// Build Arrow schema from column metadata
	columns := iter.Columns()
	if len(columns) == 0 {
		if err := iter.Close(); err != nil {
			return nil, s.ErrorHelper.WrapIO(err, "failed to execute schema query")
		}
		return arrow.NewSchema([]arrow.Field{}, nil), nil
	}

	fields := make([]arrow.Field, len(columns))
	for i, col := range columns {
		arrowType := gocqlTypeToArrow(col.TypeInfo)
		fields[i] = arrow.Field{
			Name:     col.Name,
			Type:     arrowType,
			Nullable: true,
		}
	}

	schema := arrow.NewSchema(fields, nil)
	if err := iter.Close(); err != nil {
		return nil, s.ErrorHelper.WrapIO(err, "failed to execute schema query")
	}
	return schema, nil
}

func (s *statementImpl) executeBoundQuery(ctx context.Context) (array.RecordReader, int64, error) {
	if s.query == "" {
		return nil, -1, adbc.Error{
			Code: adbc.StatusInvalidState,
			Msg:  "[cassandra] no query set",
		}
	}
	stream := s.params
	s.params = nil
	if err := s.validateParameterCount(stream.Schema().NumFields()); err != nil {
		stream.Release()
		return nil, -1, err
	}
	reader, err := newQueryRecordReader(ctx, s.connectionImpl.session, s.query, s.pageSize, stream, s.connectionImpl.Alloc, s.connectionImpl.Logger, s.ErrorHelper)
	if err != nil {
		return nil, -1, s.ErrorHelper.WrapIO(err, "failed to create bound query record reader")
	}
	return reader, -1, nil
}

func (s *statementImpl) executeBoundUpdate(ctx context.Context) (int64, error) {
	if s.query == "" {
		return -1, adbc.Error{
			Code: adbc.StatusInvalidState,
			Msg:  "[cassandra] no query set",
		}
	}

	stream := s.params
	s.params = nil
	defer stream.Release()
	if err := s.validateParameterCount(stream.Schema().NumFields()); err != nil {
		return -1, err
	}

	binders, err := makeParamBinders(stream.Schema())
	if err != nil {
		return -1, err
	}
	params := make([]any, len(binders))

	for stream.Next() {
		record := stream.RecordBatch()
		for rowIdx := range int(record.NumRows()) {
			if err := fillBindParams(binders, record, rowIdx, params); err != nil {
				return -1, err
			}
			if err := s.connectionImpl.session.Query(s.query, params...).ExecContext(ctx); err != nil {
				return -1, s.ErrorHelper.WrapIO(err, "failed to execute bound update")
			}
		}
	}
	if err := stream.Err(); err != nil {
		return -1, s.ErrorHelper.WrapIO(err, "failed to read bind stream")
	}

	return -1, nil
}

func (s *statementImpl) validateParameterCount(actualParams int) error {
	if s.query == "" {
		return nil
	}
	expectedParams := strings.Count(s.query, "?")
	if expectedParams != actualParams {
		return adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  fmt.Sprintf("[cassandra] parameter count mismatch: query has %d placeholders but %d parameters provided", expectedParams, actualParams),
		}
	}
	return nil
}

func schemaFromColumns(columns []gocql.ColumnInfo) *arrow.Schema {
	fields := make([]arrow.Field, len(columns))
	for i, col := range columns {
		fields[i] = arrow.Field{
			Name:     col.Name,
			Type:     gocqlTypeToArrow(col.TypeInfo),
			Nullable: true,
		}
	}
	return arrow.NewSchema(fields, nil)
}

func getValueFromColumn(col arrow.Array, row int) (any, error) {
	if col.IsNull(row) {
		return nil, nil
	}

	switch arr := col.(type) {
	case *array.Boolean:
		return arr.Value(row), nil
	case *array.Int8:
		return arr.Value(row), nil
	case *array.Int16:
		return arr.Value(row), nil
	case *array.Int32:
		return arr.Value(row), nil
	case *array.Int64:
		return arr.Value(row), nil
	case *array.Uint8:
		return arr.Value(row), nil
	case *array.Uint16:
		return arr.Value(row), nil
	case *array.Uint32:
		return arr.Value(row), nil
	case *array.Uint64:
		return arr.Value(row), nil
	case *array.Float32:
		return arr.Value(row), nil
	case *array.Float64:
		return arr.Value(row), nil
	case *array.Float16:
		return arr.Value(row).Float32(), nil
	case *array.String:
		return arr.Value(row), nil
	case *array.StringView:
		return arr.Value(row), nil
	case *array.LargeString:
		return arr.Value(row), nil
	case *array.Binary:
		return arr.Value(row), nil
	case *array.BinaryView:
		return arr.Value(row), nil
	case *array.LargeBinary:
		return arr.Value(row), nil
	case *array.FixedSizeBinary:
		return arr.Value(row), nil
	case *array.Timestamp:
		return timestampToMillis(int64(arr.Value(row)), arr.DataType().(*arrow.TimestampType).Unit), nil
	case *array.Time32:
		return time32ToNanos(int64(arr.Value(row)), arr.DataType().(*arrow.Time32Type).Unit), nil
	case *array.Time64:
		return time64ToNanos(int64(arr.Value(row)), arr.DataType().(*arrow.Time64Type).Unit), nil
	case *array.Date32:
		return int64(arr.Value(row)) * 24 * 60 * 60 * 1000, nil
	case *array.Date64:
		return int64(arr.Value(row)), nil
	case *array.Decimal128:
		dt := arr.DataType().(*arrow.Decimal128Type)
		return inf.NewDecBig(arr.Value(row).BigInt(), inf.Scale(dt.Scale)), nil
	case *array.List:
		// gocql marshals a Go slice into a Cassandra list<T> or set<T>.
		values := arr.ListValues()
		start, end := arr.ValueOffsets(row)
		items := make([]any, 0, end-start)
		for i := start; i < end; i++ {
			item, err := getValueFromColumn(values, int(i))
			if err != nil {
				return nil, err
			}
			items = append(items, item)
		}
		return items, nil
	case *array.Map:
		// gocql marshals a Go map into a Cassandra map<K,V>.
		keys := arr.Keys()
		items := arr.Items()
		start, end := arr.ValueOffsets(row)
		out := make(map[any]any, end-start)
		for i := start; i < end; i++ {
			key, err := getValueFromColumn(keys, int(i))
			if err != nil {
				return nil, err
			}
			value, err := getValueFromColumn(items, int(i))
			if err != nil {
				return nil, err
			}
			out[key] = value
		}
		return out, nil
	default:
		return nil, fmt.Errorf("unsupported column type: %T", col)
	}
}

func floorDiv(value int64, divisor int64) int64 {
	quotient := value / divisor
	if value < 0 && value%divisor != 0 {
		quotient--
	}
	return quotient
}

func timestampToMillis(value int64, unit arrow.TimeUnit) int64 {
	switch unit {
	case arrow.Second:
		return value * 1000
	case arrow.Millisecond:
		return value
	case arrow.Microsecond:
		return floorDiv(value, 1000)
	case arrow.Nanosecond:
		return floorDiv(value, 1000000)
	default:
		return value
	}
}

func time32ToNanos(value int64, unit arrow.TimeUnit) int64 {
	switch unit {
	case arrow.Second:
		return value * 1000000000
	case arrow.Millisecond:
		return value * 1000000
	default:
		return value
	}
}

func time64ToNanos(value int64, unit arrow.TimeUnit) int64 {
	switch unit {
	case arrow.Microsecond:
		return value * 1000
	case arrow.Nanosecond:
		return value
	default:
		return value
	}
}
