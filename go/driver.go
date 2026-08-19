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

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

const (
	// Connection options
	OptionStringHosts    = "cassandra.hosts"
	OptionStringKeyspace = "cassandra.keyspace"
	OptionStringPort     = "cassandra.port"

	// Authentication options
	OptionStringAuthUsername = "cassandra.auth.username"
	OptionStringAuthPassword = "cassandra.auth.password"

	// Connection pool options
	OptionIntNumConns       = "cassandra.num_conns"
	OptionIntPageSize       = "cassandra.page_size"
	OptionStringConsistency = "cassandra.consistency"

	// Timeout options (in milliseconds)
	OptionIntConnectTimeout = "cassandra.connect_timeout"
	OptionIntTimeout        = "cassandra.timeout"

	// TLS/SSL options
	OptionBoolEnableTLS     = "cassandra.enable_tls"
	OptionStringTLSCertPath = "cassandra.tls.cert_path"
	OptionStringTLSKeyPath  = "cassandra.tls.key_path"
	OptionStringTLSCAPath   = "cassandra.tls.ca_path"
	OptionBoolTLSSkipVerify = "cassandra.tls.skip_verify"
	OptionStringTLSHostname = "cassandra.tls.hostname_override"

	// Protocol version
	OptionIntProtocolVersion = "cassandra.protocol_version"

	// Default values
	DefaultHost            = "127.0.0.1"
	DefaultPort            = "9042"
	DefaultPageSize        = 5000
	DefaultNumConns        = 2
	DefaultConnectTimeout  = 10000 // ms
	DefaultTimeout         = 10000 // ms
	DefaultProtocolVersion = 4
	DefaultConsistency     = "LOCAL_QUORUM"
)

type driverImpl struct {
	driverbase.DriverImplBase
}

// NewDriver creates a new Cassandra driver using the given Arrow allocator.
func NewDriver(alloc memory.Allocator) driverbase.DriverWithContext {
	info := driverbase.DefaultDriverInfo("Cassandra")
	info.MustRegister(map[adbc.InfoCode]any{
		adbc.InfoDriverName:      "ADBC Driver Foundry Driver for Cassandra",
		adbc.InfoVendorSql:       false,
		adbc.InfoVendorSubstrait: false,
	})
	base := driverbase.NewDriverImplBase(info, alloc)
	base.ErrorHelper.DriverName = "cassandra"
	base.ErrorHelper.ErrorInspector = CassandraErrorInspector{}
	return driverbase.NewDriver(&driverImpl{
		DriverImplBase: base,
	})
}

func (d *driverImpl) NewDatabaseWithContext(ctx context.Context, opts map[string]string) (adbc.DatabaseWithContext, error) {
	dbBase, err := driverbase.NewDatabaseImplBase(ctx, &d.DriverImplBase, driverbase.TracingOptions{})
	if err != nil {
		return nil, err
	}
	db := &databaseImpl{
		DatabaseImplBase: dbBase,
		hosts:            []string{DefaultHost},
		port:             DefaultPort,
		pageSize:         DefaultPageSize,
		numConns:         DefaultNumConns,
		connectTimeout:   DefaultConnectTimeout,
		timeout:          DefaultTimeout,
		protocolVersion:  DefaultProtocolVersion,
		consistency:      DefaultConsistency,
	}
	if err := db.SetOptions(ctx, opts); err != nil {
		return nil, err
	}

	return driverbase.NewDatabase(db), nil
}
