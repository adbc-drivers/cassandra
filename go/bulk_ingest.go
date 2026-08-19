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
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	gocql "github.com/apache/cassandra-gocql-driver/v2"
)

const (
	maxIngestBatchCells = 256
	maxIngestBatchBytes = 16 * 1024
)

type primaryKeySpec struct {
	partition  []string
	clustering []string
}

func (p primaryKeySpec) allColumns() []string {
	columns := make([]string, 0, len(p.partition)+len(p.clustering))
	columns = append(columns, p.partition...)
	columns = append(columns, p.clustering...)
	return columns
}

// extractPrimaryKeySpec reconstructs the Cassandra primary-key layout from Arrow metadata.
func (s *statementImpl) extractPrimaryKeySpec(schema *arrow.Schema) (primaryKeySpec, error) {
	type keyColumn struct {
		name      string
		position  int
		schemaIdx int
	}

	getMetadataValue := func(metadata arrow.Metadata, key string) (string, bool) {
		for i := range metadata.Len() {
			if metadata.Keys()[i] == key {
				return metadata.Values()[i], true
			}
		}
		return "", false
	}

	var partition []keyColumn
	var clustering []keyColumn
	for i, field := range schema.Fields() {
		kind, ok := getMetadataValue(field.Metadata, "cassandra.kind")
		if !ok {
			continue
		}

		position := i
		if positionValue, ok := getMetadataValue(field.Metadata, "cassandra.position"); ok {
			parsed, err := strconv.Atoi(positionValue)
			if err != nil {
				return primaryKeySpec{}, adbc.Error{
					Code: adbc.StatusInvalidArgument,
					Msg:  fmt.Sprintf("[cassandra] invalid cassandra.position for field %s: %q", field.Name, positionValue),
				}
			}
			position = parsed
		}

		column := keyColumn{name: field.Name, position: position, schemaIdx: i}
		switch strings.ToLower(kind) {
		case "partition_key":
			partition = append(partition, column)
		case "clustering":
			clustering = append(clustering, column)
		}
	}

	if len(partition) > 0 || len(clustering) > 0 {
		if len(partition) == 0 {
			return primaryKeySpec{}, adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  "[cassandra] clustering columns require at least one partition key column",
			}
		}
		sort.SliceStable(partition, func(i, j int) bool {
			if partition[i].position != partition[j].position {
				return partition[i].position < partition[j].position
			}
			return partition[i].schemaIdx < partition[j].schemaIdx
		})
		sort.SliceStable(clustering, func(i, j int) bool {
			if clustering[i].position != clustering[j].position {
				return clustering[i].position < clustering[j].position
			}
			return clustering[i].schemaIdx < clustering[j].schemaIdx
		})

		spec := primaryKeySpec{
			partition:  make([]string, len(partition)),
			clustering: make([]string, len(clustering)),
		}
		for i, column := range partition {
			spec.partition[i] = column.name
		}
		for i, column := range clustering {
			spec.clustering[i] = column.name
		}
		return spec, nil
	}

	if schema.Metadata().Len() > 0 {
		for i := range schema.Metadata().Len() {
			key := schema.Metadata().Keys()[i]
			if key == "cassandra:primary_key" || key == "PRIMARY_KEY" {
				pkValue := schema.Metadata().Values()[i]
				columns := strings.Split(pkValue, ",")
				for i := range columns {
					columns[i] = strings.TrimSpace(columns[i])
				}
				for _, col := range columns {
					if len(schema.FieldIndices(col)) == 0 {
						return primaryKeySpec{}, adbc.Error{
							Code: adbc.StatusInvalidArgument,
							Msg:  fmt.Sprintf("[cassandra] PRIMARY KEY column '%s' not found in schema", col),
						}
					}
				}
				return primaryKeySpec{
					partition:  []string{columns[0]},
					clustering: columns[1:],
				}, nil
			}
		}
	}

	var primaryKeyColumns []string
	for _, field := range schema.Fields() {
		if field.Metadata.Len() > 0 {
			for i := range field.Metadata.Len() {
				key := field.Metadata.Keys()[i]
				value := field.Metadata.Values()[i]
				if (key == "cassandra:primary_key" || key == "PRIMARY_KEY") && (value == "true" || value == "1") {
					primaryKeyColumns = append(primaryKeyColumns, field.Name)
				}
			}
		}
	}

	if len(primaryKeyColumns) > 0 {
		return primaryKeySpec{
			partition:  []string{primaryKeyColumns[0]},
			clustering: primaryKeyColumns[1:],
		}, nil
	}

	return primaryKeySpec{}, adbc.Error{
		Code: adbc.StatusInvalidArgument,
		Msg:  "[cassandra] primary key metadata is required for bulk ingest; set schema metadata 'cassandra:primary_key' or field metadata 'cassandra.kind=partition_key'",
	}
}

// createTableFromSchema creates a table from an Arrow schema.
func (s *statementImpl) createTableFromSchema(ctx context.Context, schema *arrow.Schema, keySpec primaryKeySpec) error {
	query, err := s.buildCreateTableQuery(schema, keySpec, false)
	if err != nil {
		return err
	}
	if err := s.connectionImpl.session.Query(query).ExecContext(ctx); err != nil {
		return s.ErrorHelper.WrapIO(err, "failed to create table")
	}

	return nil
}

// createTableIfNotExists creates a table if it doesn't already exist.
func (s *statementImpl) createTableIfNotExists(ctx context.Context, schema *arrow.Schema, keySpec primaryKeySpec) error {
	query, err := s.buildCreateTableQuery(schema, keySpec, true)
	if err != nil {
		return err
	}
	if err := s.connectionImpl.session.Query(query).ExecContext(ctx); err != nil {
		return s.ErrorHelper.WrapIO(err, "failed to create table")
	}
	return nil
}

func (s *statementImpl) buildCreateTableQuery(schema *arrow.Schema, keySpec primaryKeySpec, ifNotExists bool) (string, error) {
	var createQuery strings.Builder
	createQuery.WriteString("CREATE TABLE ")
	if ifNotExists {
		createQuery.WriteString("IF NOT EXISTS ")
	}
	createQuery.WriteString(quoteIdentifier(s.ingest.targetTable))
	createQuery.WriteString(" (")

	for i, field := range schema.Fields() {
		if i > 0 {
			createQuery.WriteString(", ")
		}
		createQuery.WriteString(quoteIdentifier(field.Name))
		createQuery.WriteString(" ")
		createQuery.WriteString(s.arrowTypeToCassandraType(field.Type))
	}

	if len(keySpec.partition) == 0 {
		return "", adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  "[cassandra] partition key columns not specified for table creation",
		}
	}

	createQuery.WriteString(", ")
	createQuery.WriteString(buildPrimaryKeyClause(keySpec))
	createQuery.WriteString(")")
	return createQuery.String(), nil
}

func buildPrimaryKeyClause(keySpec primaryKeySpec) string {
	var clause strings.Builder
	clause.WriteString("PRIMARY KEY (")
	if len(keySpec.partition) == 1 {
		clause.WriteString(quoteIdentifier(keySpec.partition[0]))
	} else {
		clause.WriteString("(")
		clause.WriteString(joinQuoted(keySpec.partition, ", "))
		clause.WriteString(")")
	}
	if len(keySpec.clustering) > 0 {
		clause.WriteString(", ")
		clause.WriteString(joinQuoted(keySpec.clustering, ", "))
	}
	clause.WriteString(")")
	return clause.String()
}

// arrowTypeToCassandraType converts Arrow data type to a Cassandra type string.
func (s *statementImpl) arrowTypeToCassandraType(dt arrow.DataType) string {
	switch dt.ID() {
	case arrow.INT8:
		return "TINYINT"
	case arrow.INT16:
		return "SMALLINT"
	case arrow.INT32:
		return "INT"
	case arrow.INT64:
		return "BIGINT"
	case arrow.UINT8:
		return "TINYINT"
	case arrow.UINT16:
		return "SMALLINT"
	case arrow.UINT32:
		return "INT"
	case arrow.UINT64:
		return "BIGINT"
	case arrow.FLOAT32:
		return "FLOAT"
	case arrow.FLOAT64:
		return "DOUBLE"
	case arrow.STRING:
		return "TEXT"
	case arrow.BINARY, arrow.BINARY_VIEW, arrow.LARGE_BINARY, arrow.FIXED_SIZE_BINARY:
		return "BLOB"
	case arrow.BOOL:
		return "BOOLEAN"
	case arrow.TIMESTAMP:
		return "TIMESTAMP"
	case arrow.DATE32, arrow.DATE64:
		return "DATE"
	case arrow.TIME32, arrow.TIME64:
		return "TIME"
	case arrow.DECIMAL, arrow.DECIMAL256:
		return "DECIMAL"
	case arrow.LIST:
		// An Arrow list maps to a CQL list<T>. Element types recurse; a frozen
		// wrapper is required so the collection can be nested and, in general,
		// used without per-element update semantics.
		lt := dt.(*arrow.ListType)
		return fmt.Sprintf("LIST<%s>", s.frozenIfNested(lt.Elem()))
	case arrow.MAP:
		mt := dt.(*arrow.MapType)
		return fmt.Sprintf("MAP<%s, %s>", s.frozenIfNested(mt.KeyType()), s.frozenIfNested(mt.ItemType()))
	default:
		return "TEXT"
	}
}

// frozenIfNested returns the CQL type for a collection element, wrapping nested
// collections in FROZEN<...> as Cassandra requires for collections inside
// collections.
func (s *statementImpl) frozenIfNested(dt arrow.DataType) string {
	inner := s.arrowTypeToCassandraType(dt)
	switch dt.ID() {
	case arrow.LIST, arrow.MAP:
		return fmt.Sprintf("FROZEN<%s>", inner)
	default:
		return inner
	}
}

// ExecuteIngest implements bulk ingestion.
func (s *statementImpl) ExecuteIngest(ctx context.Context, reader array.RecordReader) (int64, error) {
	if s.ingest.targetTable == "" {
		return -1, adbc.Error{
			Code: adbc.StatusInvalidState,
			Msg:  "[cassandra] target table not set for ingestion",
		}
	}

	var skippedRows int64
	firstBatch := true
	var keySpec primaryKeySpec

	for reader.Next() {
		record := reader.RecordBatch()

		if firstBatch {
			schema := record.Schema()

			extracted, err := s.extractPrimaryKeySpec(schema)
			if err != nil {
				return -1, err
			}
			keySpec = extracted

			mode := s.ingest.mode
			if mode == "" {
				mode = adbc.OptionValueIngestModeCreateAppend
			}

			switch mode {
			case adbc.OptionValueIngestModeCreate:
				if err := s.createTableFromSchema(ctx, schema, keySpec); err != nil {
					return -1, err
				}
			case adbc.OptionValueIngestModeReplace:
				dropQuery := fmt.Sprintf("DROP TABLE IF EXISTS %s", quoteIdentifier(s.ingest.targetTable))
				if err := s.connectionImpl.session.Query(dropQuery).ExecContext(ctx); err != nil {
					return -1, s.ErrorHelper.WrapIO(err, "failed to drop table")
				}
				if err := s.createTableFromSchema(ctx, schema, keySpec); err != nil {
					return -1, err
				}
			case adbc.OptionValueIngestModeCreateAppend:
				if err := s.createTableIfNotExists(ctx, schema, keySpec); err != nil {
					return -1, err
				}
			case adbc.OptionValueIngestModeAppend:
			}
			firstBatch = false
		}

		numRows := record.NumRows()
		schema := record.Schema()
		columnNames := make([]string, schema.NumFields())
		for i := range schema.NumFields() {
			columnNames[i] = schema.Field(i).Name
		}

		insertQuery := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
			quoteIdentifier(s.ingest.targetTable),
			joinQuoted(columnNames, ", "),
			placeholders(len(columnNames)))

		batch := s.connectionImpl.session.Batch(gocql.UnloggedBatch)
		batchEntryLimit := ingestBatchEntryLimit(schema.NumFields())
		batchBytes := 0
		flushBatch := func() error {
			if err := s.executeIngestBatch(ctx, batch); err != nil {
				return err
			}
			batch = s.connectionImpl.session.Batch(gocql.UnloggedBatch)
			batchBytes = 0
			return nil
		}

		for rowIdx := range int(numRows) {
			hasNullPrimaryKey := false
			for _, pkColName := range keySpec.allColumns() {
				colIdx := schema.FieldIndices(pkColName)[0]
				col := record.Column(colIdx)
				if col.IsNull(rowIdx) {
					hasNullPrimaryKey = true
					break
				}
			}

			if hasNullPrimaryKey {
				skippedRows++
				continue
			}

			values := make([]any, schema.NumFields())
			for colIdx := range schema.NumFields() {
				col := record.Column(colIdx)
				value, err := getValueFromColumn(col, rowIdx)
				if err != nil {
					return -1, adbc.Error{
						Code: adbc.StatusInvalidData,
						Msg:  fmt.Sprintf("[cassandra] failed to extract value: %v", err),
					}
				}
				values[colIdx] = value
			}
			rowBytes := ingestRowSize(values)
			if rowBytes >= maxIngestBatchBytes {
				if err := flushBatch(); err != nil {
					return -1, err
				}
				if err := s.executeIngestQuery(ctx, insertQuery, values); err != nil {
					return -1, err
				}
				continue
			}

			if len(batch.Entries) > 0 &&
				(len(batch.Entries) >= batchEntryLimit || batchBytes+rowBytes > maxIngestBatchBytes) {
				if err := flushBatch(); err != nil {
					return -1, err
				}
			}

			batch.Query(insertQuery, values...)
			batchBytes += rowBytes
		}

		if err := s.executeIngestBatch(ctx, batch); err != nil {
			return -1, err
		}
	}

	if err := reader.Err(); err != nil {
		return -1, s.ErrorHelper.WrapIO(err, "failed to read records")
	}

	if skippedRows > 0 {
		return -1, adbc.Error{
			Code: adbc.StatusInvalidData,
			Msg:  fmt.Sprintf("[cassandra] cannot insert rows with NULL PRIMARY KEY columns (skipped %d rows)", skippedRows),
		}
	}

	return -1, nil
}

func (s *statementImpl) executeIngestQuery(ctx context.Context, query string, values []any) error {
	if err := s.connectionImpl.session.Query(query, values...).ExecContext(ctx); err != nil {
		return s.ErrorHelper.WrapIO(err, "failed to execute insert")
	}
	return nil
}

func (s *statementImpl) executeIngestBatch(ctx context.Context, batch *gocql.Batch) error {
	if len(batch.Entries) == 0 {
		return nil
	}
	if err := batch.ExecContext(ctx); err != nil {
		return s.ErrorHelper.WrapIO(err, "failed to execute batch insert")
	}
	return nil
}

func ingestBatchEntryLimit(numFields int) int {
	if numFields <= 0 {
		return 1
	}
	limit := maxIngestBatchCells / numFields
	if limit < 1 {
		return 1
	}
	return limit
}

func ingestRowSize(values []any) int {
	size := 0
	for _, value := range values {
		switch value := value.(type) {
		case nil:
			size++
		case string:
			size += len(value)
		case []byte:
			size += len(value)
		default:
			size += 16
		}
	}
	return size
}

func joinQuoted(strs []string, sep string) string {
	quoted := make([]string, len(strs))
	for i, s := range strs {
		quoted[i] = quoteIdentifier(s)
	}
	return join(quoted, sep)
}

func join(strs []string, sep string) string {
	if len(strs) == 0 {
		return ""
	}
	var result strings.Builder
	result.WriteString(strs[0])
	for i := 1; i < len(strs); i++ {
		result.WriteString(sep + strs[i])
	}
	return result.String()
}

func placeholders(count int) string {
	if count == 0 {
		return ""
	}
	var result strings.Builder
	result.WriteString("?")
	for i := 1; i < count; i++ {
		result.WriteString(", ?")
	}
	return result.String()
}
