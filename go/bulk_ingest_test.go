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
	"errors"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBulkIngestExtractPrimaryKeySpecFromFieldMetadata(t *testing.T) {
	stmt := &statementImpl{}
	schema := arrow.NewSchema([]arrow.Field{
		{
			Name:     "ck_b",
			Type:     arrow.BinaryTypes.String,
			Metadata: arrow.MetadataFrom(map[string]string{"cassandra.kind": "clustering", "cassandra.position": "1"}),
		},
		{
			Name:     "pk_b",
			Type:     arrow.BinaryTypes.String,
			Metadata: arrow.MetadataFrom(map[string]string{"cassandra.kind": "partition_key", "cassandra.position": "1"}),
		},
		{
			Name:     "value",
			Type:     arrow.BinaryTypes.String,
			Metadata: arrow.MetadataFrom(map[string]string{"cassandra.kind": "regular", "cassandra.position": "-1"}),
		},
		{
			Name:     "ck_a",
			Type:     arrow.PrimitiveTypes.Int32,
			Metadata: arrow.MetadataFrom(map[string]string{"cassandra.kind": "clustering", "cassandra.position": "0"}),
		},
		{
			Name:     "pk_a",
			Type:     arrow.BinaryTypes.String,
			Metadata: arrow.MetadataFrom(map[string]string{"cassandra.kind": "partition_key", "cassandra.position": "0"}),
		},
	}, nil)

	keySpec, err := stmt.extractPrimaryKeySpec(schema)
	require.NoError(t, err)
	assert.Equal(t, []string{"pk_a", "pk_b"}, keySpec.partition)
	assert.Equal(t, []string{"ck_a", "ck_b"}, keySpec.clustering)
	assert.Equal(t, []string{"pk_a", "pk_b", "ck_a", "ck_b"}, keySpec.allColumns())
}

func TestBulkIngestBuildCreateTableQueryUsesCompositePartitionKey(t *testing.T) {
	stmt := &statementImpl{}
	stmt.ingest.targetTable = "events"
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "pk_a", Type: arrow.BinaryTypes.String},
		{Name: "pk_b", Type: arrow.PrimitiveTypes.Int32},
		{Name: "ck_a", Type: arrow.PrimitiveTypes.Int64},
		{Name: "value", Type: arrow.BinaryTypes.String},
	}, nil)

	query, err := stmt.buildCreateTableQuery(schema, primaryKeySpec{
		partition:  []string{"pk_a", "pk_b"},
		clustering: []string{"ck_a"},
	}, false)
	require.NoError(t, err)
	assert.Equal(t, `CREATE TABLE "events" ("pk_a" TEXT, "pk_b" INT, "ck_a" BIGINT, "value" TEXT, PRIMARY KEY (("pk_a", "pk_b"), "ck_a"))`, query)
}

func TestBulkIngestBuildCreateTableQueryUsesSinglePartitionKey(t *testing.T) {
	stmt := &statementImpl{}
	stmt.ingest.targetTable = "events"
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "pk", Type: arrow.BinaryTypes.String},
		{Name: "ck_a", Type: arrow.PrimitiveTypes.Int32},
		{Name: "ck_b", Type: arrow.PrimitiveTypes.Int64},
		{Name: "value", Type: arrow.BinaryTypes.String},
	}, nil)

	query, err := stmt.buildCreateTableQuery(schema, primaryKeySpec{
		partition:  []string{"pk"},
		clustering: []string{"ck_a", "ck_b"},
	}, true)
	require.NoError(t, err)
	assert.Equal(t, `CREATE TABLE IF NOT EXISTS "events" ("pk" TEXT, "ck_a" INT, "ck_b" BIGINT, "value" TEXT, PRIMARY KEY ("pk", "ck_a", "ck_b"))`, query)
}

func TestBulkIngestExtractPrimaryKeySpecFallsBackToLegacyMetadata(t *testing.T) {
	stmt := &statementImpl{}
	metadata := arrow.MetadataFrom(map[string]string{"PRIMARY_KEY": "part, idx"})
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "part", Type: arrow.BinaryTypes.String},
		{Name: "idx", Type: arrow.PrimitiveTypes.Int32},
		{Name: "value", Type: arrow.BinaryTypes.String},
	}, &metadata)

	keySpec, err := stmt.extractPrimaryKeySpec(schema)
	require.NoError(t, err)
	assert.Equal(t, []string{"part"}, keySpec.partition)
	assert.Equal(t, []string{"idx"}, keySpec.clustering)
}

func TestBulkIngestExtractPrimaryKeySpecRequiresMetadata(t *testing.T) {
	stmt := &statementImpl{}
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "part", Type: arrow.BinaryTypes.String},
		{Name: "value", Type: arrow.BinaryTypes.String},
	}, nil)

	_, err := stmt.extractPrimaryKeySpec(schema)
	require.Error(t, err)

	var adbcErr adbc.Error
	require.True(t, errors.As(err, &adbcErr))
	assert.Equal(t, adbc.StatusInvalidArgument, adbcErr.Code)
	assert.Contains(t, adbcErr.Msg, "primary key metadata is required")
	assert.Contains(t, adbcErr.Msg, "cassandra:primary_key")
}

func TestBulkIngestExtractPrimaryKeySpecRejectsClusteringWithoutPartition(t *testing.T) {
	stmt := &statementImpl{}
	schema := arrow.NewSchema([]arrow.Field{
		{
			Name:     "ck_a",
			Type:     arrow.PrimitiveTypes.Int32,
			Metadata: arrow.MetadataFrom(map[string]string{"cassandra.kind": "clustering", "cassandra.position": "0"}),
		},
	}, nil)

	_, err := stmt.extractPrimaryKeySpec(schema)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "clustering columns require at least one partition key")
}

func TestIngestRowSize(t *testing.T) {
	values := []any{"abc", []byte{1, 2}, int64(42), nil}
	assert.Equal(t, 22, ingestRowSize(values))
}
