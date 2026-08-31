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
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	gocql "github.com/apache/cassandra-gocql-driver/v2"
)

type connectionImpl struct {
	driverbase.ConnectionImplBase

	session  *gocql.Session
	cluster  *gocql.ClusterConfig
	keyspace string
	pageSize int
	version  string
}

type cassandraColumn struct {
	tableName  string
	columnName string
	columnType string
	kind       string
	position   int
}

func (c *connectionImpl) Close(ctx context.Context) error {
	if c.session != nil {
		c.session.Close()
		c.session = nil
	}
	return nil
}

func (c *connectionImpl) PrepareDriverInfo(ctx context.Context, infoCodes []adbc.InfoCode) error {
	if c.version == "" {
		if err := c.session.Query("SELECT release_version FROM system.local").ScanContext(ctx, &c.version); err != nil {
			return c.ErrorHelper.WrapIO(err, "failed to get Cassandra version")
		}
	}
	return c.DriverInfo.RegisterInfoCode(adbc.InfoVendorVersion, c.version)
}

func (c *connectionImpl) Commit(ctx context.Context) error {
	// Cassandra doesn't support transactions
	return adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "[cassandra] transactions are not supported",
	}
}

func (c *connectionImpl) Rollback(ctx context.Context) error {
	// Cassandra doesn't support transactions
	return adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "[cassandra] transactions are not supported",
	}
}

func (c *connectionImpl) NewStatement(ctx context.Context) (adbc.StatementWithContext, error) {
	stmt := &statementImpl{
		StatementImplBase: driverbase.NewStatementImplBase(&c.ConnectionImplBase, c.ErrorHelper),
		connectionImpl:    c,
		pageSize:          c.pageSize,
	}
	return driverbase.NewStatement(stmt), nil
}

func (c *connectionImpl) ReadPartition(ctx context.Context, serializedPartition []byte) (array.RecordReader, error) {
	return nil, adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "[cassandra] ReadPartition not implemented",
	}
}

// CurrentNamespacer implementation
func (c *connectionImpl) GetCurrentCatalog(ctx context.Context) (string, error) {
	// Cassandra doesn't have catalogs
	return "", nil
}

func (c *connectionImpl) GetCurrentDbSchema(ctx context.Context) (string, error) {
	return c.keyspace, nil
}

func (c *connectionImpl) SetCurrentCatalog(ctx context.Context, catalog string) error {
	// Cassandra doesn't have catalogs
	return adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "[cassandra] catalogs are not supported",
	}
}

func (c *connectionImpl) SetCurrentDbSchema(ctx context.Context, schema string) error {
	return adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "[cassandra] changing the current schema/keyspace after opening a connection is not supported",
	}
}

// TableTypeLister implementation
func (c *connectionImpl) ListTableTypes(ctx context.Context) ([]string, error) {
	return []string{"TABLE", "MATERIALIZED VIEW"}, nil
}

// GetTableSchema implementation
func (c *connectionImpl) GetTableSchema(ctx context.Context, catalog *string, dbSchema *string, tableName string) (*arrow.Schema, error) {
	// Use current keyspace if dbSchema is not specified
	keyspace := c.keyspace
	if dbSchema != nil && *dbSchema != "" {
		keyspace = *dbSchema
	}

	if keyspace == "" {
		return nil, adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  "[cassandra] no keyspace specified",
		}
	}

	// Reconstruct logical schema order from kind/position metadata instead of
	// relying on system_schema.columns scan order.
	query := "SELECT column_name, type, kind, position FROM system_schema.columns WHERE keyspace_name = ? AND table_name = ?"
	iter := c.session.Query(query, keyspace, tableName).IterContext(ctx)

	var columns []cassandraColumn
	var columnName, columnType, kind string
	var position int

	for iter.Scan(&columnName, &columnType, &kind, &position) {
		columns = append(columns, cassandraColumn{
			columnName: columnName,
			columnType: columnType,
			kind:       kind,
			position:   position,
		})
	}

	if err := iter.Close(); err != nil {
		return nil, c.ErrorHelper.WrapIO(err, "failed to get table schema")
	}

	if len(columns) == 0 {
		return nil, adbc.Error{
			Code: adbc.StatusNotFound,
			Msg:  fmt.Sprintf("[cassandra] table %s.%s not found", keyspace, tableName),
		}
	}

	columns = orderCassandraColumns(columns)

	fields := make([]arrow.Field, 0, len(columns))
	for _, column := range columns {
		arrowType := cassandraTypeToArrow(column.columnType)

		fields = append(fields, arrow.Field{
			Name:     column.columnName,
			Type:     arrowType,
			Nullable: true,
			Metadata: fieldMetadataForColumn(column),
		})
	}

	return arrow.NewSchema(fields, nil), nil
}

// DbObjectsEnumerator implementation
func (c *connectionImpl) GetCatalogs(ctx context.Context, catalogFilter *string) ([]string, error) {
	// Cassandra doesn't have catalogs, so return a single empty-string catalog
	// This allows driverbase to descend into schemas/tables for catalog-less databases
	if catalogFilter != nil && !matchPattern("", *catalogFilter) {
		return nil, nil
	}
	return []string{""}, nil
}

func (c *connectionImpl) GetDBSchemasForCatalog(ctx context.Context, catalog string, schemaFilter *string) ([]string, error) {
	query := "SELECT keyspace_name FROM system_schema.keyspaces"

	iter := c.session.Query(query).IterContext(ctx)

	var schemas []string
	var keyspaceName string
	for iter.Scan(&keyspaceName) {
		// Filter out system keyspaces
		if strings.HasPrefix(keyspaceName, "system") {
			continue
		}
		if schemaFilter != nil && *schemaFilter != "" && !matchPattern(keyspaceName, *schemaFilter) {
			continue
		}
		schemas = append(schemas, keyspaceName)
	}

	if err := iter.Close(); err != nil {
		return nil, c.ErrorHelper.WrapIO(err, "failed to list keyspaces")
	}

	return schemas, nil
}

func (c *connectionImpl) GetTablesForDBSchema(ctx context.Context, catalog string, schema string, tableFilter *string, columnFilter *string, includeColumns bool) ([]driverbase.TableInfo, error) {
	query := "SELECT table_name FROM system_schema.tables WHERE keyspace_name = ?"
	args := []any{schema}
	columnInfosByTable := map[string][]driverbase.ColumnInfo{}

	if includeColumns {
		var err error
		columnInfosByTable, err = c.getColumnsForKeyspace(ctx, schema, columnFilter)
		if err != nil {
			return nil, err
		}
	}

	iter := c.session.Query(query, args...).IterContext(ctx)

	var tables []driverbase.TableInfo
	var tableName string
	for iter.Scan(&tableName) {
		if tableFilter != nil && *tableFilter != "" && !matchPattern(tableName, *tableFilter) {
			continue
		}

		tableInfo := driverbase.TableInfo{
			TableName: tableName,
			TableType: "TABLE",
		}

		// Get columns if requested
		if includeColumns {
			tableInfo.TableColumns = columnInfosByTable[tableName]
		}

		tables = append(tables, tableInfo)
	}

	if err := iter.Close(); err != nil {
		return nil, c.ErrorHelper.WrapIO(err, "failed to list tables")
	}

	// Get materialized views
	viewQuery := "SELECT view_name FROM system_schema.views WHERE keyspace_name = ?"
	viewIter := c.session.Query(viewQuery, schema).IterContext(ctx)

	var viewName string
	for viewIter.Scan(&viewName) {
		if tableFilter != nil && *tableFilter != "" && !matchPattern(viewName, *tableFilter) {
			continue
		}

		tableInfo := driverbase.TableInfo{
			TableName: viewName,
			TableType: "MATERIALIZED VIEW",
		}

		if includeColumns {
			tableInfo.TableColumns = columnInfosByTable[viewName]
		}

		tables = append(tables, tableInfo)
	}

	if err := viewIter.Close(); err != nil {
		return nil, c.ErrorHelper.WrapIO(err, "failed to list views")
	}

	return tables, nil
}

func (c *connectionImpl) getColumnsForKeyspace(ctx context.Context, keyspace string, columnFilter *string) (map[string][]driverbase.ColumnInfo, error) {
	query := "SELECT table_name, column_name, type, kind, position FROM system_schema.columns WHERE keyspace_name = ?"
	iter := c.session.Query(query, keyspace).IterContext(ctx)

	var rawColumns []cassandraColumn
	var tableName, columnName, columnType, kind string
	var position int

	for iter.Scan(&tableName, &columnName, &columnType, &kind, &position) {
		rawColumns = append(rawColumns, cassandraColumn{
			tableName:  tableName,
			columnName: columnName,
			columnType: columnType,
			kind:       kind,
			position:   position,
		})
	}

	if err := iter.Close(); err != nil {
		return nil, c.ErrorHelper.WrapIO(err, "failed to get keyspace columns")
	}

	return buildColumnInfosByTable(rawColumns, columnFilter), nil
}

func orderCassandraColumns(columns []cassandraColumn) []cassandraColumn {
	ordered := append([]cassandraColumn(nil), columns...)
	sort.SliceStable(ordered, func(i, j int) bool {
		left, right := ordered[i], ordered[j]
		if leftKind, rightKind := cassandraColumnKindRank(left.kind), cassandraColumnKindRank(right.kind); leftKind != rightKind {
			return leftKind < rightKind
		}
		if leftPos, rightPos := cassandraColumnPosition(left), cassandraColumnPosition(right); leftPos != rightPos {
			return leftPos < rightPos
		}
		return left.columnName < right.columnName
	})
	return ordered
}

func buildColumnInfos(columns []cassandraColumn, columnFilter *string) []driverbase.ColumnInfo {
	infos := make([]driverbase.ColumnInfo, 0, len(columns))
	for idx, column := range columns {
		if columnFilter != nil && *columnFilter != "" && !matchPattern(column.columnName, *columnFilter) {
			continue
		}

		pos := int32(idx + 1) // ADBC expects 1-based ordinal positions.
		remarks := column.kind
		infos = append(infos, driverbase.ColumnInfo{
			ColumnName:      column.columnName,
			OrdinalPosition: &pos,
			Remarks:         &remarks,
		})
	}
	return infos
}

func buildColumnInfosByTable(columns []cassandraColumn, columnFilter *string) map[string][]driverbase.ColumnInfo {
	grouped := make(map[string][]cassandraColumn)
	for _, column := range columns {
		grouped[column.tableName] = append(grouped[column.tableName], column)
	}

	infosByTable := make(map[string][]driverbase.ColumnInfo, len(grouped))
	for tableName, tableColumns := range grouped {
		infosByTable[tableName] = buildColumnInfos(orderCassandraColumns(tableColumns), columnFilter)
	}
	return infosByTable
}

func cassandraColumnKindRank(kind string) int {
	switch strings.ToLower(kind) {
	case "partition_key":
		return 0
	case "clustering":
		return 1
	case "static":
		return 2
	case "regular":
		return 3
	default:
		return 4
	}
}

func cassandraColumnPosition(column cassandraColumn) int {
	if column.position >= 0 {
		return column.position
	}
	return int(^uint(0) >> 1)
}

func fieldMetadataForColumn(column cassandraColumn) arrow.Metadata {
	// Add standard SQL metadata plus Cassandra-specific key metadata.
	metadata := map[string]string{
		"sql.column_name":        column.columnName,
		"sql.database_type_name": column.columnType,
		"cassandra.kind":         column.kind,
		"cassandra.position":     strconv.Itoa(column.position),
	}
	return arrow.MetadataFrom(metadata)
}

// Helper functions
func quoteIdentifier(identifier string) string {
	return "\"" + strings.ReplaceAll(identifier, "\"", "\"\"") + "\""
}

func matchPattern(value, pattern string) bool {
	if pattern == "%" || pattern == "" {
		return true
	}

	var builder strings.Builder
	builder.Grow(len(pattern) * 2)
	builder.WriteString("^")
	for _, ch := range pattern {
		switch ch {
		case '%':
			builder.WriteString(".*")
		case '_':
			builder.WriteString(".")
		default:
			builder.WriteString(regexp.QuoteMeta(string(ch)))
		}
	}
	builder.WriteString("$")

	matched, err := regexp.MatchString(builder.String(), value)
	if err != nil {
		return false
	}
	return matched
}

const (
	defaultDecimalPrecision int32 = 38
	defaultDecimalScale     int32 = 10
)

func defaultDecimalType() *arrow.Decimal128Type {
	return &arrow.Decimal128Type{Precision: defaultDecimalPrecision, Scale: defaultDecimalScale}
}

func cassandraTypeToArrow(cassandraType string) arrow.DataType {
	// Map Cassandra types to Arrow types
	cassandraType = strings.ToLower(cassandraType)

	switch {
	case strings.HasPrefix(cassandraType, "text"), strings.HasPrefix(cassandraType, "varchar"), strings.HasPrefix(cassandraType, "ascii"):
		return arrow.BinaryTypes.String
	case cassandraType == "int":
		return arrow.PrimitiveTypes.Int32
	case cassandraType == "bigint", cassandraType == "counter":
		return arrow.PrimitiveTypes.Int64
	case cassandraType == "smallint":
		return arrow.PrimitiveTypes.Int16
	case cassandraType == "tinyint":
		return arrow.PrimitiveTypes.Int8
	case cassandraType == "float":
		return arrow.PrimitiveTypes.Float32
	case cassandraType == "double":
		return arrow.PrimitiveTypes.Float64
	case cassandraType == "boolean":
		return arrow.FixedWidthTypes.Boolean
	case cassandraType == "timestamp":
		return &arrow.TimestampType{Unit: arrow.Millisecond}
	case cassandraType == "date":
		return arrow.FixedWidthTypes.Date32
	case cassandraType == "time":
		return &arrow.Time64Type{Unit: arrow.Nanosecond}
	case cassandraType == "uuid", cassandraType == "timeuuid":
		return arrow.BinaryTypes.String
	case cassandraType == "blob":
		return arrow.BinaryTypes.Binary
	case strings.HasPrefix(cassandraType, "decimal"):
		// Cassandra does not expose column-level decimal precision or scale.
		return defaultDecimalType()
	case strings.HasPrefix(cassandraType, "list<"), strings.HasPrefix(cassandraType, "set<"):
		// list<T> and set<T> both map to an Arrow list (Cassandra has no Arrow
		// set type, and a set carries no expressible ordering guarantee).
		elem := collectionInner(cassandraType)
		if elem == "" {
			return arrow.BinaryTypes.String
		}
		return arrow.ListOf(cassandraTypeToArrow(elem))
	case strings.HasPrefix(cassandraType, "vector<"):
		inner := collectionInner(cassandraType)
		elem, dimensionsText, ok := splitMapTypes(inner)
		if !ok {
			return arrow.BinaryTypes.String
		}
		dimensions, err := strconv.ParseInt(dimensionsText, 10, 32)
		if err != nil || dimensions <= 0 {
			return arrow.BinaryTypes.String
		}
		return arrow.FixedSizeListOf(int32(dimensions), cassandraTypeToArrow(elem))
	case strings.HasPrefix(cassandraType, "map<"):
		inner := collectionInner(cassandraType)
		key, value, ok := splitMapTypes(inner)
		if !ok {
			return arrow.BinaryTypes.String
		}
		return arrow.MapOf(cassandraTypeToArrow(key), cassandraTypeToArrow(value))
	default:
		return arrow.BinaryTypes.String
	}
}

// collectionInner returns the type arguments inside the angle brackets of a
// collection type string, e.g. "list<text>" -> "text" and
// "map<text, int>" -> "text, int". It returns "" if no brackets are present.
func collectionInner(cassandraType string) string {
	start := strings.Index(cassandraType, "<")
	end := strings.LastIndex(cassandraType, ">")
	if start == -1 || end == -1 || end <= start+1 {
		return ""
	}
	return strings.TrimSpace(cassandraType[start+1 : end])
}

// splitMapTypes splits a map's inner "key, value" argument list on the top-level
// comma, ignoring commas nested inside angle brackets (e.g. a nested collection
// value type). It returns the key type, value type, and whether the split
// succeeded.
func splitMapTypes(inner string) (string, string, bool) {
	depth := 0
	for i, r := range inner {
		switch r {
		case '<':
			depth++
		case '>':
			depth--
		case ',':
			if depth == 0 {
				return strings.TrimSpace(inner[:i]), strings.TrimSpace(inner[i+1:]), true
			}
		}
	}
	return "", "", false
}
