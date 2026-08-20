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

package cassandra_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/adbc-drivers/driverbase-go/validation"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	cassandra "github.com/adbc-drivers/cassandra/go"
	gocql "github.com/apache/cassandra-gocql-driver/v2"
)

// CassandraQuirks implements validation.DriverQuirks for Cassandra ADBC driver
type CassandraQuirks struct {
	hosts    string
	keyspace string
	mem      *memory.CheckedAllocator
}

func cassandraTestConfig() (string, string) {
	hosts := os.Getenv("CASSANDRA_HOSTS")
	if hosts == "" {
		hosts = "127.0.0.1"
	}
	keyspace := os.Getenv("CASSANDRA_KEYSPACE")
	if keyspace == "" {
		keyspace = "adbc_test"
	}
	return hosts, keyspace
}

func cassandraDialTargets(hosts string) []string {
	targets := make([]string, 0, 1)
	for host := range strings.SplitSeq(hosts, ",") {
		host = strings.TrimSpace(host)
		if host == "" {
			continue
		}

		if _, _, err := net.SplitHostPort(host); err == nil {
			targets = append(targets, host)
			continue
		}
		targets = append(targets, net.JoinHostPort(host, cassandra.DefaultPort))
	}
	return targets
}

func requireCassandraIntegration(t testing.TB) (string, string) {
	t.Helper()

	hosts, keyspace := cassandraTestConfig()
	dialer := net.Dialer{Timeout: 500 * time.Millisecond}
	failures := make([]string, 0)

	for _, target := range cassandraDialTargets(hosts) {
		conn, err := dialer.Dial("tcp", target)
		if err == nil {
			_ = conn.Close()
			return hosts, keyspace
		}
		failures = append(failures, fmt.Sprintf("%s (%v)", target, err))
	}

	t.Skipf(
		"skipping Cassandra integration tests: no reachable Cassandra host for %q: %s",
		hosts,
		strings.Join(failures, ", "),
	)
	return "", ""
}

func (q *CassandraQuirks) SetupDriver(t *testing.T) driverbase.DriverWithContext {
	q.mem = memory.NewCheckedAllocator(memory.DefaultAllocator)
	return cassandra.NewDriver(q.mem)
}

func (q *CassandraQuirks) TearDownDriver(t *testing.T, _ driverbase.DriverWithContext) {
	q.mem.AssertSize(t, 0)
}

func (q *CassandraQuirks) DatabaseOptions() map[string]string {
	return map[string]string{
		cassandra.OptionStringHosts:    q.hosts,
		cassandra.OptionStringKeyspace: q.keyspace,
	}
}

func (q *CassandraQuirks) CreateSampleTable(tableName string, r arrow.RecordBatch) error {
	// Use gocql to create table directly
	cluster := gocql.NewCluster(strings.Split(q.hosts, ",")...)
	cluster.Keyspace = q.keyspace
	session, err := cluster.CreateSession()
	if err != nil {
		return err
	}
	defer session.Close()

	// Drop table if it exists first to ensure clean state
	err = session.Query("DROP TABLE IF EXISTS " + tableName).Exec()
	if err != nil {
		return fmt.Errorf("failed to drop existing table: %w", err)
	}

	// Build CREATE TABLE statement based on Arrow schema
	var createQuery strings.Builder
	createQuery.WriteString("CREATE TABLE ")
	createQuery.WriteString(tableName)
	createQuery.WriteString(" (")

	schema := r.Schema()
	for i, field := range schema.Fields() {
		if i > 0 {
			createQuery.WriteString(", ")
		}
		createQuery.WriteString(field.Name)
		createQuery.WriteString(" ")

		// Map Arrow types to Cassandra types
		switch field.Type.ID() {
		case arrow.INT8:
			createQuery.WriteString("TINYINT")
		case arrow.INT16:
			createQuery.WriteString("SMALLINT")
		case arrow.INT32:
			createQuery.WriteString("INT")
		case arrow.INT64:
			createQuery.WriteString("BIGINT")
		case arrow.STRING:
			createQuery.WriteString("TEXT")
		case arrow.FLOAT32:
			createQuery.WriteString("FLOAT")
		case arrow.FLOAT64:
			createQuery.WriteString("DOUBLE")
		case arrow.BOOL:
			createQuery.WriteString("BOOLEAN")
		case arrow.TIMESTAMP:
			createQuery.WriteString("TIMESTAMP")
		case arrow.DATE32, arrow.DATE64:
			createQuery.WriteString("DATE")
		case arrow.BINARY:
			createQuery.WriteString("BLOB")
		default:
			createQuery.WriteString("TEXT") // Default fallback
		}
	}

	// Cassandra requires PRIMARY KEY - use first column
	if schema.NumFields() > 0 {
		createQuery.WriteString(", PRIMARY KEY (")
		createQuery.WriteString(schema.Field(0).Name)
		createQuery.WriteString(")")
	}
	createQuery.WriteString(")")

	err = session.Query(createQuery.String()).Exec()
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	// Insert data from Arrow record
	if r.NumRows() > 0 {
		// Build INSERT statement
		var insertQuery strings.Builder
		insertQuery.WriteString("INSERT INTO ")
		insertQuery.WriteString(tableName)
		insertQuery.WriteString(" (")

		fieldNames := make([]string, schema.NumFields())
		for i, field := range schema.Fields() {
			if i > 0 {
				insertQuery.WriteString(", ")
			}
			insertQuery.WriteString(field.Name)
			fieldNames[i] = field.Name
		}
		insertQuery.WriteString(") VALUES (")

		placeholders := make([]string, schema.NumFields())
		for i := range placeholders {
			placeholders[i] = "?"
		}
		insertQuery.WriteString(strings.Join(placeholders, ", "))
		insertQuery.WriteString(")")

		// Insert each row
		for row := range r.NumRows() {
			// Skip rows where PRIMARY KEY (first column) is NULL
			firstCol := r.Column(0)
			if firstCol.IsNull(int(row)) {
				continue
			}

			values := make([]any, r.NumCols())
			for col := range r.NumCols() {
				column := r.Column(int(col))
				if column.IsNull(int(row)) {
					values[col] = nil
				} else {
					// Extract value based on column type
					switch arr := column.(type) {
					case *array.Int8:
						values[col] = arr.Value(int(row))
					case *array.Int16:
						values[col] = arr.Value(int(row))
					case *array.Int32:
						values[col] = arr.Value(int(row))
					case *array.Int64:
						values[col] = arr.Value(int(row))
					case *array.String:
						values[col] = arr.Value(int(row))
					case *array.Float32:
						values[col] = arr.Value(int(row))
					case *array.Float64:
						values[col] = arr.Value(int(row))
					case *array.Boolean:
						values[col] = arr.Value(int(row))
					case *array.Binary:
						values[col] = arr.Value(int(row))
					case *array.Timestamp:
						values[col] = arr.Value(int(row)).ToTime(arr.DataType().(*arrow.TimestampType).Unit)
					default:
						values[col] = fmt.Sprintf("%v", column)
					}
				}
			}

			err = session.Query(insertQuery.String(), values...).Exec()
			if err != nil {
				return fmt.Errorf("failed to insert row %d: %w", row, err)
			}
		}
	}

	return nil
}

func (q *CassandraQuirks) DropTable(cnxn adbc.ConnectionWithContext, tblName string) error {
	ctx := context.Background()
	stmt, err := cnxn.NewStatement(ctx)
	if err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, stmt.Close(ctx))
	}()

	if err = stmt.SetSqlQuery(ctx, "DROP TABLE IF EXISTS "+tblName); err != nil {
		return err
	}

	_, err = stmt.ExecuteUpdate(ctx)
	return err
}

func (q *CassandraQuirks) SampleTableSchemaMetadata(tblName string, dt arrow.DataType) arrow.Metadata {
	// Return metadata that matches what our Cassandra type converter actually returns
	metadata := map[string]string{}

	switch dt.ID() {
	case arrow.INT8:
		metadata["sql.column_name"] = "tinyints"
		metadata["sql.database_type_name"] = "tinyint"
	case arrow.INT16:
		metadata["sql.column_name"] = "smallints"
		metadata["sql.database_type_name"] = "smallint"
	case arrow.INT32:
		metadata["sql.column_name"] = "ints"
		metadata["sql.database_type_name"] = "int"
	case arrow.INT64:
		metadata["sql.column_name"] = "ints"
		metadata["sql.database_type_name"] = "bigint"
	case arrow.STRING:
		metadata["sql.column_name"] = "strings"
		metadata["sql.database_type_name"] = "text"
	case arrow.FLOAT32:
		metadata["sql.column_name"] = "floats"
		metadata["sql.database_type_name"] = "float"
	case arrow.FLOAT64:
		metadata["sql.column_name"] = "doubles"
		metadata["sql.database_type_name"] = "double"
	case arrow.BOOL:
		metadata["sql.column_name"] = "bools"
		metadata["sql.database_type_name"] = "boolean"
	}

	// The validation sample table uses the first column as the partition key.
	switch metadata["sql.column_name"] {
	case "ints":
		metadata["cassandra.kind"] = "partition_key"
		metadata["cassandra.position"] = "0"
	case "strings":
		metadata["cassandra.kind"] = "regular"
		metadata["cassandra.position"] = "-1"
	}

	return arrow.MetadataFrom(metadata)
}

func (q *CassandraQuirks) Alloc() memory.Allocator      { return q.mem }
func (q *CassandraQuirks) BindParameter(idx int) string { return "?" }
func (q *CassandraQuirks) QuoteTableName(name string) string {
	return "\"" + strings.ReplaceAll(name, "\"", "\"\"") + "\""
}

func (q *CassandraQuirks) SupportsBulkIngest(string) bool              { return false }
func (q *CassandraQuirks) SupportsConcurrentStatements() bool          { return false }
func (q *CassandraQuirks) SupportsCurrentCatalogSchema() bool          { return true }
func (q *CassandraQuirks) SupportsGetTableSchema() bool                { return true }
func (q *CassandraQuirks) SupportsExecuteSchema() bool                 { return false }
func (q *CassandraQuirks) SupportsGetSetOptions() bool                 { return true }
func (q *CassandraQuirks) SupportsPartitionedData() bool               { return false }
func (q *CassandraQuirks) SupportsStatistics() bool                    { return false }
func (q *CassandraQuirks) SupportsTransactions() bool                  { return false }
func (q *CassandraQuirks) SupportsGetParameterSchema() bool            { return false }
func (q *CassandraQuirks) SupportsDynamicParameterBinding() bool       { return true }
func (q *CassandraQuirks) SupportsErrorIngestIncompatibleSchema() bool { return true }
func (q *CassandraQuirks) Catalog() string                             { return "" }
func (q *CassandraQuirks) DBSchema() string                            { return "adbc_test" }

func (q *CassandraQuirks) GetMetadata(code adbc.InfoCode) any {
	switch code {
	case adbc.InfoDriverName:
		return "ADBC Driver Foundry Driver for Cassandra"
	case adbc.InfoDriverVersion:
		return "(unknown or development build)"
	case adbc.InfoDriverArrowVersion:
		return "v18.7.0"
	case adbc.InfoVendorVersion:
		return regexp.MustCompile(`5\.0\.[0-9]+`)
	case adbc.InfoVendorArrowVersion:
		return "(unknown or development build)"
	case adbc.InfoDriverADBCVersion:
		return adbc.AdbcVersion1_1_0
	case adbc.InfoVendorName:
		return "Cassandra"
	case adbc.InfoVendorSql:
		return false
	case adbc.InfoVendorSubstrait:
		return false
	}
	return nil
}

func withQuirks(t *testing.T, fn func(*CassandraQuirks)) {
	hosts, keyspace := cassandraTestConfig()
	q := &CassandraQuirks{hosts: hosts, keyspace: keyspace}
	fn(q)
}

type CassandraStatementTests struct {
	validation.StatementTests
}

func (s *CassandraStatementTests) TestBindExecuteUpdate() {
	s.T().Skip("generic validation uses SELECT without FROM, which is invalid CQL")
}

func (s *CassandraStatementTests) TestBindStreamExecuteUpdate() {
	s.T().Skip("generic validation uses SELECT without FROM, which is invalid CQL")
}

func (s *CassandraStatementTests) TestSQLPrepareSelectNoParams() {
	s.T().Skip("generic validation uses SELECT without FROM, which is invalid CQL")
}

func (s *CassandraStatementTests) TestSQLPrepareSelectParams() {
	s.T().Skip("generic validation uses SELECT without FROM, which is invalid CQL")
}

// TestValidation runs the comprehensive ADBC validation test suite
// This is the primary test that validates ADBC specification compliance
func TestValidation(t *testing.T) {
	requireCassandraIntegration(t)
	withQuirks(t, func(q *CassandraQuirks) {
		suite.Run(t, &validation.DatabaseTests{Quirks: q})
		suite.Run(t, &validation.ConnectionTests{Quirks: q})
		suite.Run(t, &CassandraStatementTests{
			Quirks: q,
		})
	})
}

// -------------------- Additional Tests --------------------

type CassandraTests struct {
	suite.Suite

	Quirks *CassandraQuirks

	ctx    context.Context
	driver driverbase.DriverWithContext
	db     adbc.DatabaseWithContext
	cnxn   adbc.ConnectionWithContext
	stmt   adbc.StatementWithContext
}

func (s *CassandraTests) SetupTest() {
	// Driver/DB/Connection/Statement are set up externally in TestCassandraTypeTests
	// Nothing to do here
}

func (s *CassandraTests) TearDownTest() {
	// Cleanup is handled externally in TestCassandraTypeTests
	// Nothing to do here
}

type selectCase struct {
	name     string
	query    string
	schema   *arrow.Schema
	expected string
}

func (s *CassandraTests) TestSelect() {
	// Create test table with various Cassandra types
	s.NoError(s.stmt.SetSqlQuery(s.ctx, `
		CREATE TABLE IF NOT EXISTS test_types (
			id INT PRIMARY KEY,
			tinyint_col TINYINT,
			smallint_col SMALLINT,
			int_col INT,
			bigint_col BIGINT,
			float_col FLOAT,
			double_col DOUBLE,
			text_col TEXT,
			bool_col BOOLEAN,
			blob_col BLOB
		)
	`))
	_, err := s.stmt.ExecuteUpdate(s.ctx)
	s.NoError(err)

	// Insert test data
	s.NoError(s.stmt.SetSqlQuery(s.ctx, `
		INSERT INTO test_types (id, tinyint_col, smallint_col, int_col, bigint_col,
								float_col, double_col, text_col, bool_col, blob_col)
		VALUES (1, 42, 1000, 12345, 9876543210, 3.25, 6.75, 'hello world', true, 0xdeadbeef)
	`))
	_, err = s.stmt.ExecuteUpdate(s.ctx)
	s.NoError(err)

	for _, testCase := range []selectCase{
		{
			name:  "tinyint",
			query: "SELECT tinyint_col AS value FROM test_types WHERE id = 1",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "value",
					Type:     arrow.PrimitiveTypes.Int8,
					Nullable: true,
				},
			}, nil),
			expected: `[{"value": 42}]`,
		},
		{
			name:  "smallint",
			query: "SELECT smallint_col AS value FROM test_types WHERE id = 1",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "value",
					Type:     arrow.PrimitiveTypes.Int16,
					Nullable: true,
				},
			}, nil),
			expected: `[{"value": 1000}]`,
		},
		{
			name:  "int32",
			query: "SELECT int_col AS theanswer FROM test_types WHERE id = 1",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "theanswer",
					Type:     arrow.PrimitiveTypes.Int32,
					Nullable: true,
				},
			}, nil),
			expected: `[{"theanswer": 12345}]`,
		},
		{
			name:  "int64",
			query: "SELECT bigint_col AS theanswer FROM test_types WHERE id = 1",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "theanswer",
					Type:     arrow.PrimitiveTypes.Int64,
					Nullable: true,
				},
			}, nil),
			expected: `[{"theanswer": 9876543210}]`,
		},
		{
			name:  "float32",
			query: "SELECT float_col AS value FROM test_types WHERE id = 1",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "value",
					Type:     arrow.PrimitiveTypes.Float32,
					Nullable: true,
				},
			}, nil),
			expected: `[{"value": 3.25}]`,
		},
		{
			name:  "float64",
			query: "SELECT double_col AS value FROM test_types WHERE id = 1",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "value",
					Type:     arrow.PrimitiveTypes.Float64,
					Nullable: true,
				},
			}, nil),
			expected: `[{"value": 6.75}]`,
		},
		{
			name:  "string",
			query: "SELECT text_col AS greeting FROM test_types WHERE id = 1",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "greeting",
					Type:     arrow.BinaryTypes.String,
					Nullable: true,
				},
			}, nil),
			expected: `[{"greeting": "hello world"}]`,
		},
		{
			name:  "boolean",
			query: "SELECT bool_col AS istrue FROM test_types WHERE id = 1",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "istrue",
					Type:     arrow.FixedWidthTypes.Boolean,
					Nullable: true,
				},
			}, nil),
			expected: `[{"istrue": true}]`,
		},
	} {
		s.Run(testCase.name, func() {
			s.NoError(s.stmt.SetSqlQuery(s.ctx, testCase.query))

			rdr, rows, err := s.stmt.ExecuteQuery(s.ctx)
			s.NoError(err)
			defer rdr.Release()

			s.Truef(testCase.schema.Equal(rdr.Schema()), "expected: %s\ngot: %s", testCase.schema, rdr.Schema())
			s.Equal(int64(-1), rows)
			s.Truef(rdr.Next(), "no record, error? %s", rdr.Err())

			expectedRecord, _, err := array.RecordFromJSON(s.Quirks.Alloc(), testCase.schema, bytes.NewReader([]byte(testCase.expected)))
			s.NoError(err)
			defer expectedRecord.Release()

			rec := rdr.RecordBatch()
			s.NotNil(rec)

			s.Truef(array.RecordEqual(expectedRecord, rec), "expected: %s\ngot: %s", expectedRecord, rec)

			s.False(rdr.Next())
			s.NoError(rdr.Err())
		})
	}
}

type CassandraTestSuite struct {
	suite.Suite
	hosts    string
	keyspace string
	ctx      context.Context
	driver   driverbase.DriverWithContext
	db       adbc.DatabaseWithContext
	cnxn     adbc.ConnectionWithContext
	stmt     adbc.StatementWithContext
}

func (s *CassandraTestSuite) SetupSuite() {
	var err error
	s.hosts = os.Getenv("CASSANDRA_HOSTS")
	if s.hosts == "" {
		s.hosts = "127.0.0.1"
	}
	s.keyspace = os.Getenv("CASSANDRA_KEYSPACE")
	if s.keyspace == "" {
		s.keyspace = "adbc_test"
	}

	s.ctx = context.Background()

	// Use default allocator for integration tests
	// CheckedAllocator is used by validation suite, this is just additional testing
	s.driver = cassandra.NewDriver(memory.DefaultAllocator)
	s.db, err = s.driver.NewDatabaseWithContext(s.ctx, map[string]string{
		cassandra.OptionStringHosts:    s.hosts,
		cassandra.OptionStringKeyspace: s.keyspace,
	})
	s.NoError(err)

	s.cnxn, err = s.db.Open(s.ctx)
	s.NoError(err)

	s.stmt, err = s.cnxn.NewStatement(s.ctx)
	s.NoError(err)
}

func (s *CassandraTestSuite) TearDownSuite() {
	if s.stmt != nil {
		s.NoError(s.stmt.Close(s.ctx))
	}
	if s.cnxn != nil {
		s.NoError(s.cnxn.Close(s.ctx))
	}
	if s.db != nil {
		s.NoError(s.db.Close(s.ctx))
	}
}

func (s *CassandraTestSuite) TestBulkIngestManyColumns() {
	const numCols = 50 // Cassandra has lower limits than MySQL
	const numRows = 5
	tableName := "bulk_ingest_wide"

	// Drop the table if it exists
	s.NoError(s.stmt.SetSqlQuery(s.ctx, "DROP TABLE IF EXISTS "+tableName))
	_, err := s.stmt.ExecuteUpdate(s.ctx)
	s.Require().NoError(err)

	// Build a schema with 50 int64 columns (first one is PRIMARY KEY)
	fields := make([]arrow.Field, numCols)
	for i := range numCols {
		fields[i] = arrow.Field{
			Name: fmt.Sprintf("col_%d", i), Type: arrow.PrimitiveTypes.Int64, Nullable: true,
		}
	}
	metadata := arrow.MetadataFrom(map[string]string{"cassandra:primary_key": "col_0"})
	schema := arrow.NewSchema(fields, &metadata)

	// Create table explicitly for Cassandra
	var createQuery strings.Builder
	createQuery.WriteString("CREATE TABLE ")
	createQuery.WriteString(tableName)
	createQuery.WriteString(" (")
	for i := range numCols {
		if i > 0 {
			createQuery.WriteString(", ")
		}
		fmt.Fprintf(&createQuery, "col_%d BIGINT", i)
	}
	createQuery.WriteString(", PRIMARY KEY (col_0))")

	s.Require().NoError(s.stmt.SetSqlQuery(s.ctx, createQuery.String()))
	_, err = s.stmt.ExecuteUpdate(s.ctx)
	s.Require().NoError(err)

	// Build a record batch with a few rows
	batchbldr := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer batchbldr.Release()
	for col := range numCols {
		bldr := batchbldr.Field(col).(*array.Int64Builder)
		for row := range numRows {
			bldr.Append(int64(col*numRows + row))
		}
	}
	batch := batchbldr.NewRecordBatch()
	defer batch.Release()

	// Ingest
	stmt, err := s.cnxn.NewStatement(s.ctx)
	s.Require().NoError(err)
	defer func() { s.NoError(stmt.Close(s.ctx)) }()

	s.Require().NoError(stmt.SetOption(s.ctx, adbc.OptionKeyIngestTargetTable, tableName))
	s.Require().NoError(stmt.Bind(s.ctx, batch))

	affected, err := stmt.ExecuteUpdate(s.ctx)
	s.Require().NoError(err)
	if affected != -1 {
		s.EqualValues(numRows, affected)
	}

	// Verify the data was ingested correctly
	s.Require().NoError(stmt.SetSqlQuery(s.ctx, "SELECT COUNT(*) FROM "+tableName))
	rdr, _, err := stmt.ExecuteQuery(s.ctx)
	s.Require().NoError(err)
	defer rdr.Release()

	s.Require().True(rdr.Next())
	count := rdr.RecordBatch().Column(0).(*array.Int64).Value(0)
	s.EqualValues(numRows, count)
}

func (s *CassandraTestSuite) TestMetadataOperations() {
	// Test GetTableTypes
	rdr, err := s.cnxn.GetTableTypes(s.ctx)
	s.NoError(err)
	defer rdr.Release()

	tableTypes := []string{}
	for rdr.Next() {
		record := rdr.RecordBatch()
		arr := record.Column(0)
		for i := range int(record.NumRows()) {
			if !arr.IsNull(i) {
				tableTypes = append(tableTypes, arr.ValueStr(i))
			}
		}
	}
	s.NoError(rdr.Err())
	s.Contains(tableTypes, "TABLE")

	// Test current namespace
	connOptions, ok := s.cnxn.(adbc.GetSetOptionsWithContext)
	s.Require().True(ok)

	currentSchema, err := connOptions.GetOption(s.ctx, adbc.OptionKeyCurrentDbSchema)
	s.NoError(err)
	s.Equal(s.keyspace, currentSchema)

	// Catalog should be empty
	currentCatalog, err := connOptions.GetOption(s.ctx, adbc.OptionKeyCurrentCatalog)
	s.NoError(err)
	s.Empty(currentCatalog)
}

func TestCassandraTypeTests(t *testing.T) {
	hosts, keyspace := requireCassandraIntegration(t)

	// Use quirks without CheckedAllocator for type tests to avoid test infrastructure leaks
	quirks := &CassandraQuirks{
		hosts:    hosts,
		keyspace: keyspace,
		mem:      memory.NewCheckedAllocator(memory.DefaultAllocator),
	}

	// Create test suite with default allocator driver
	driver := cassandra.NewDriver(memory.DefaultAllocator)
	ctx := context.Background()
	db, err := driver.NewDatabaseWithContext(ctx, quirks.DatabaseOptions())
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close(ctx)) }()

	cnxn, err := db.Open(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, cnxn.Close(ctx)) }()

	stmt, err := cnxn.NewStatement(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, stmt.Close(ctx)) }()

	// Run tests using default allocator
	testSuite := &CassandraTests{
		Quirks: quirks,
		ctx:    ctx,
		driver: driver,
		db:     db,
		cnxn:   cnxn,
		stmt:   stmt,
	}
	suite.Run(t, testSuite)
}

func TestCassandraIntegrationSuite(t *testing.T) {
	hosts, keyspace := requireCassandraIntegration(t)
	suite.Run(t, &CassandraTestSuite{
		hosts:    hosts,
		keyspace: keyspace,
	})
}

func TestCassandraDialTargets(t *testing.T) {
	t.Run("adds default port", func(t *testing.T) {
		assert.Equal(t, []string{"127.0.0.1:9042"}, cassandraDialTargets("127.0.0.1"))
	})

	t.Run("preserves explicit ports and trims whitespace", func(t *testing.T) {
		assert.Equal(
			t,
			[]string{"db1:9042", "db2:9142"},
			cassandraDialTargets(" db1 , db2:9142 "),
		)
	})
}

// TestURIParsing tests the URI parsing functionality
func TestURIParsing(t *testing.T) {
	tests := []struct {
		name        string
		uri         string
		username    string
		password    string
		expectError bool
	}{
		{
			name: "basic uri",
			uri:  "cassandra://localhost:9042/testkeyspace",
		},
		{
			name: "uri with credentials",
			uri:  "cassandra://user:pass@localhost:9042/testkeyspace",
		},
		{
			name: "uri without port",
			uri:  "cassandra://localhost/testkeyspace",
		},
		{
			name:        "invalid scheme",
			uri:         "mysql://localhost/test",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mem := memory.NewGoAllocator()
			driver := cassandra.NewDriver(mem)

			opts := map[string]string{
				adbc.OptionKeyURI: tt.uri,
			}
			if tt.username != "" {
				opts[adbc.OptionKeyUsername] = tt.username
			}
			if tt.password != "" {
				opts[adbc.OptionKeyPassword] = tt.password
			}

			db, err := driver.NewDatabaseWithContext(context.Background(), opts)

			if tt.expectError {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.NotNil(t, db)
			require.NoError(t, db.Close(context.Background()))
		})
	}
}

func TestURIQueryParameters(t *testing.T) {
	ctx := context.Background()
	driver := cassandra.NewDriver(memory.NewGoAllocator())
	db, err := driver.NewDatabaseWithContext(ctx, map[string]string{
		adbc.OptionKeyURI: "cassandra://localhost/testkeyspace?" +
			"num_conns=3&page_size=2048&consistency=ONE&" +
			"connect_timeout=1500&timeout=2500&enable_tls=true&" +
			"tls_cert_path=%2Ftmp%2Fclient+cert.pem&" +
			"tls_key_path=%2Ftmp%2Fclient.key&tls_ca_path=%2Ftmp%2Fca.pem&" +
			"tls_skip_verify=false&tls_hostname_override=db.example.com&" +
			"protocol_version=5",
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close(ctx)) }()

	options, ok := db.(adbc.GetSetOptionsWithContext)
	require.True(t, ok)

	expected := map[string]string{
		cassandra.OptionIntNumConns:        "3",
		cassandra.OptionIntPageSize:        "2048",
		cassandra.OptionStringConsistency:  "ONE",
		cassandra.OptionIntConnectTimeout:  "1500",
		cassandra.OptionIntTimeout:         "2500",
		cassandra.OptionBoolEnableTLS:      adbc.OptionValueEnabled,
		cassandra.OptionStringTLSCertPath:  "/tmp/client cert.pem",
		cassandra.OptionStringTLSKeyPath:   "/tmp/client.key",
		cassandra.OptionStringTLSCAPath:    "/tmp/ca.pem",
		cassandra.OptionBoolTLSSkipVerify:  adbc.OptionValueDisabled,
		cassandra.OptionStringTLSHostname:  "db.example.com",
		cassandra.OptionIntProtocolVersion: "5",
	}
	for option, want := range expected {
		got, err := options.GetOption(ctx, option)
		require.NoError(t, err, option)
		assert.Equal(t, want, got, option)
	}
}

func TestURIQueryParameterErrors(t *testing.T) {
	tests := map[string]string{
		"unknown parameter":  "unknown=value",
		"repeated parameter": "page_size=100&page_size=200",
		"malformed escaping": "tls_cert_path=%zz",
		"invalid integer":    "page_size=many",
		"invalid boolean":    "enable_tls=yes",
	}

	for name, query := range tests {
		t.Run(name, func(t *testing.T) {
			driver := cassandra.NewDriver(memory.NewGoAllocator())
			db, err := driver.NewDatabaseWithContext(context.Background(), map[string]string{
				adbc.OptionKeyURI: "cassandra://localhost/testkeyspace?" + query,
			})
			require.Nil(t, db)
			require.Error(t, err)

			var adbcErr adbc.Error
			require.ErrorAs(t, err, &adbcErr)
			assert.Equal(t, adbc.StatusInvalidArgument, adbcErr.Code)
		})
	}
}

func TestURIExplicitOptionsOverride(t *testing.T) {
	mem := memory.NewGoAllocator()
	driver := cassandra.NewDriver(mem)
	ctx := context.Background()

	db, err := driver.NewDatabaseWithContext(ctx, map[string]string{
		adbc.OptionKeyURI:              "cassandra://uriuser:uripass@db1:9042/app?page_size=250",
		cassandra.OptionStringHosts:    "db2,db3",
		cassandra.OptionStringPort:     "9142",
		cassandra.OptionStringKeyspace: "override_app",
		adbc.OptionKeyUsername:         "explicit-user",
		adbc.OptionKeyPassword:         "explicit-pass",
		cassandra.OptionIntPageSize:    "1000",
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close(ctx)) }()

	options, ok := db.(adbc.GetSetOptionsWithContext)
	require.True(t, ok)

	hosts, err := options.GetOption(ctx, cassandra.OptionStringHosts)
	require.NoError(t, err)
	assert.Equal(t, "db2,db3", hosts)

	port, err := options.GetOption(ctx, cassandra.OptionStringPort)
	require.NoError(t, err)
	assert.Equal(t, "9142", port)

	keyspace, err := options.GetOption(ctx, cassandra.OptionStringKeyspace)
	require.NoError(t, err)
	assert.Equal(t, "override_app", keyspace)

	username, err := options.GetOption(ctx, cassandra.OptionStringAuthUsername)
	require.NoError(t, err)
	assert.Equal(t, "explicit-user", username)

	password, err := options.GetOption(ctx, cassandra.OptionStringAuthPassword)
	require.NoError(t, err)
	assert.Equal(t, "explicit-pass", password)

	pageSize, err := options.GetOption(ctx, cassandra.OptionIntPageSize)
	require.NoError(t, err)
	assert.Equal(t, "1000", pageSize)
}
