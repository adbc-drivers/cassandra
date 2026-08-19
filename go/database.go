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
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	gocql "github.com/apache/cassandra-gocql-driver/v2"
)

type databaseImpl struct {
	driverbase.DatabaseImplBase

	// Connection parameters
	hosts    []string
	port     string
	keyspace string

	// Authentication
	username string
	password string

	// Connection pool settings
	numConns    int
	pageSize    int
	consistency string

	// Timeouts
	connectTimeout int
	timeout        int

	// TLS settings
	enableTLS     bool
	tlsCertPath   string
	tlsKeyPath    string
	tlsCAPath     string
	tlsSkipVerify bool
	tlsHostname   string

	// Protocol version
	protocolVersion int
}

type uriQueryOption struct {
	key       string
	isBoolean bool
}

var uriQueryOptions = map[string]uriQueryOption{
	"num_conns":             {key: OptionIntNumConns},
	"page_size":             {key: OptionIntPageSize},
	"consistency":           {key: OptionStringConsistency},
	"connect_timeout":       {key: OptionIntConnectTimeout},
	"timeout":               {key: OptionIntTimeout},
	"enable_tls":            {key: OptionBoolEnableTLS, isBoolean: true},
	"tls_cert_path":         {key: OptionStringTLSCertPath},
	"tls_key_path":          {key: OptionStringTLSKeyPath},
	"tls_ca_path":           {key: OptionStringTLSCAPath},
	"tls_skip_verify":       {key: OptionBoolTLSSkipVerify, isBoolean: true},
	"tls_hostname_override": {key: OptionStringTLSHostname},
	"protocol_version":      {key: OptionIntProtocolVersion},
}

func (d *databaseImpl) Open(ctx context.Context) (adbc.ConnectionWithContext, error) {
	// Create gocql cluster configuration
	cluster := gocql.NewCluster(d.hosts...)
	cluster.Port = parsePort(d.port)
	cluster.Keyspace = d.keyspace
	cluster.NumConns = d.numConns
	cluster.PageSize = d.pageSize
	cluster.ConnectTimeout = time.Duration(d.connectTimeout) * time.Millisecond
	cluster.Timeout = time.Duration(d.timeout) * time.Millisecond
	cluster.ProtoVersion = d.protocolVersion

	// Set consistency level
	if consistency, err := parseConsistency(d.consistency); err == nil {
		cluster.Consistency = consistency
	}

	// Configure authentication
	if d.username != "" || d.password != "" {
		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: d.username,
			Password: d.password,
		}
	}

	// Configure TLS
	if d.enableTLS {
		tlsConfig, err := d.buildTLSConfig()
		if err != nil {
			return nil, adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  fmt.Sprintf("[cassandra] failed to configure TLS: %v", err),
			}
		}
		cluster.SslOpts = &gocql.SslOptions{
			Config: tlsConfig,
		}
	}

	// Create session
	session, err := cluster.CreateSession()
	if err != nil {
		return nil, d.ErrorHelper.WrapIO(err, "failed to create session")
	}

	conn := &connectionImpl{
		ConnectionImplBase: driverbase.NewConnectionImplBase(&d.DatabaseImplBase),
		session:            session,
		cluster:            cluster,
		keyspace:           d.keyspace,
		pageSize:           d.pageSize,
	}

	return driverbase.NewConnectionBuilder(conn).
		WithCurrentNamespacer(conn).
		WithDriverInfoPreparer(conn).
		WithTableTypeLister(conn).
		WithDbObjectsEnumerator(conn).
		Connection(), nil
}

func (d *databaseImpl) Close(ctx context.Context) error {
	return nil
}

func (d *databaseImpl) GetOption(ctx context.Context, key string) (string, error) {
	switch key {
	case OptionStringHosts:
		return strings.Join(d.hosts, ","), nil
	case OptionStringKeyspace:
		return d.keyspace, nil
	case OptionStringPort:
		return d.port, nil
	case OptionStringAuthUsername:
		return d.username, nil
	case OptionStringAuthPassword:
		return d.password, nil
	case OptionIntNumConns:
		return strconv.Itoa(d.numConns), nil
	case OptionIntPageSize:
		return strconv.Itoa(d.pageSize), nil
	case OptionStringConsistency:
		return d.consistency, nil
	case OptionIntConnectTimeout:
		return strconv.Itoa(d.connectTimeout), nil
	case OptionIntTimeout:
		return strconv.Itoa(d.timeout), nil
	case OptionBoolEnableTLS:
		if d.enableTLS {
			return adbc.OptionValueEnabled, nil
		}
		return adbc.OptionValueDisabled, nil
	case OptionStringTLSCertPath:
		return d.tlsCertPath, nil
	case OptionStringTLSKeyPath:
		return d.tlsKeyPath, nil
	case OptionStringTLSCAPath:
		return d.tlsCAPath, nil
	case OptionBoolTLSSkipVerify:
		if d.tlsSkipVerify {
			return adbc.OptionValueEnabled, nil
		}
		return adbc.OptionValueDisabled, nil
	case OptionStringTLSHostname:
		return d.tlsHostname, nil
	case OptionIntProtocolVersion:
		return strconv.Itoa(d.protocolVersion), nil
	default:
		return d.DatabaseImplBase.GetOption(ctx, key)
	}
}

func (d *databaseImpl) SetOption(ctx context.Context, key string, value string) error {
	switch key {
	case adbc.OptionKeyURI:
		return d.parseURI(ctx, value)
	case OptionStringHosts:
		d.hosts = strings.Split(value, ",")
		for i := range d.hosts {
			d.hosts[i] = strings.TrimSpace(d.hosts[i])
		}
	case OptionStringKeyspace:
		d.keyspace = value
	case OptionStringPort:
		d.port = value
	case adbc.OptionKeyUsername, OptionStringAuthUsername:
		d.username = value
	case adbc.OptionKeyPassword, OptionStringAuthPassword:
		d.password = value
	case OptionIntNumConns:
		val, err := strconv.Atoi(value)
		if err != nil {
			return adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  fmt.Sprintf("[cassandra] invalid num_conns value: %v", err),
			}
		}
		d.numConns = val
	case OptionIntPageSize:
		val, err := strconv.Atoi(value)
		if err != nil {
			return adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  fmt.Sprintf("[cassandra] invalid page_size value: %v", err),
			}
		}
		d.pageSize = val
	case OptionStringConsistency:
		d.consistency = value
	case OptionIntConnectTimeout:
		val, err := strconv.Atoi(value)
		if err != nil {
			return adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  fmt.Sprintf("[cassandra] invalid connect_timeout value: %v", err),
			}
		}
		d.connectTimeout = val
	case OptionIntTimeout:
		val, err := strconv.Atoi(value)
		if err != nil {
			return adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  fmt.Sprintf("[cassandra] invalid timeout value: %v", err),
			}
		}
		d.timeout = val
	case OptionBoolEnableTLS:
		d.enableTLS = value == adbc.OptionValueEnabled
	case OptionStringTLSCertPath:
		d.tlsCertPath = value
	case OptionStringTLSKeyPath:
		d.tlsKeyPath = value
	case OptionStringTLSCAPath:
		d.tlsCAPath = value
	case OptionBoolTLSSkipVerify:
		d.tlsSkipVerify = value == adbc.OptionValueEnabled
	case OptionStringTLSHostname:
		d.tlsHostname = value
	case OptionIntProtocolVersion:
		val, err := strconv.Atoi(value)
		if err != nil {
			return adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  fmt.Sprintf("[cassandra] invalid protocol_version value: %v", err),
			}
		}
		d.protocolVersion = val
	default:
		return d.DatabaseImplBase.SetOption(ctx, key, value)
	}
	return nil
}

func (d *databaseImpl) SetOptions(ctx context.Context, options map[string]string) error {
	if uri, ok := options[adbc.OptionKeyURI]; ok {
		if err := d.SetOption(ctx, adbc.OptionKeyURI, uri); err != nil {
			return err
		}
	}

	for k, v := range options {
		if k == adbc.OptionKeyURI {
			continue
		}
		if err := d.SetOption(ctx, k, v); err != nil {
			return err
		}
	}
	return nil
}

func (d *databaseImpl) parseURI(ctx context.Context, uri string) error {
	// Parse cassandra:// URI format: cassandra://[username:password@]host[:port][/keyspace][?options]
	if !strings.HasPrefix(uri, "cassandra://") {
		return adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  fmt.Sprintf("[cassandra] invalid URI scheme, expected 'cassandra://', got: %s", uri),
		}
	}

	// Remove scheme
	uri = strings.TrimPrefix(uri, "cassandra://")

	// Split on ? to separate connection string from query parameters
	parts := strings.SplitN(uri, "?", 2)
	connStr := parts[0]

	// Parse authentication if present
	if idx := strings.Index(connStr, "@"); idx != -1 {
		auth := connStr[:idx]
		connStr = connStr[idx+1:]
		authParts := strings.SplitN(auth, ":", 2)
		d.username = authParts[0]
		if len(authParts) > 1 {
			d.password = authParts[1]
		}
	}

	// Parse host, port, and keyspace
	hostAndKeyspace := strings.SplitN(connStr, "/", 2)
	hostAndPort := hostAndKeyspace[0]

	if hostAndPort != "" {
		if strings.Contains(hostAndPort, ":") {
			hostPortParts := strings.Split(hostAndPort, ":")
			d.hosts = []string{hostPortParts[0]}
			d.port = hostPortParts[1]
		} else {
			d.hosts = []string{hostAndPort}
		}
	}

	if len(hostAndKeyspace) > 1 {
		d.keyspace = hostAndKeyspace[1]
	}

	// Parse query parameters after the authority and path so they can override
	// defaults while explicit ADBC options can still override the URI.
	if len(parts) > 1 {
		if err := d.parseURIQuery(ctx, parts[1]); err != nil {
			return err
		}
	}

	return nil
}

func (d *databaseImpl) parseURIQuery(ctx context.Context, rawQuery string) error {
	values, err := url.ParseQuery(rawQuery)
	if err != nil {
		return adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  fmt.Sprintf("[cassandra] invalid URI query: %v", err),
		}
	}

	for name, queryValues := range values {
		option, ok := uriQueryOptions[name]
		if !ok {
			return adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  fmt.Sprintf("[cassandra] unknown URI query parameter %q", name),
			}
		}
		if len(queryValues) != 1 {
			return adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  fmt.Sprintf("[cassandra] URI query parameter %q must be specified once", name),
			}
		}

		value := queryValues[0]
		if option.isBoolean && value != adbc.OptionValueEnabled && value != adbc.OptionValueDisabled {
			return adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg: fmt.Sprintf(
					"[cassandra] URI query parameter %q must be %q or %q",
					name,
					adbc.OptionValueEnabled,
					adbc.OptionValueDisabled,
				),
			}
		}

		if err := d.SetOption(ctx, option.key, value); err != nil {
			return err
		}
	}

	return nil
}

func (d *databaseImpl) buildTLSConfig() (*tls.Config, error) {
	tlsConfig := &tls.Config{
		InsecureSkipVerify: d.tlsSkipVerify,
	}

	if d.tlsHostname != "" {
		tlsConfig.ServerName = d.tlsHostname
	}

	// Load CA certificate if provided
	if d.tlsCAPath != "" {
		caCert, err := os.ReadFile(d.tlsCAPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read CA certificate: %w", err)
		}
		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to parse CA certificate")
		}
		tlsConfig.RootCAs = caCertPool
	}

	// Load client certificate and key if provided
	if d.tlsCertPath != "" && d.tlsKeyPath != "" {
		cert, err := tls.LoadX509KeyPair(d.tlsCertPath, d.tlsKeyPath)
		if err != nil {
			return nil, fmt.Errorf("failed to load client certificate: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	return tlsConfig, nil
}

func parsePort(portStr string) int {
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return 9042
	}
	return port
}

func parseConsistency(consistencyStr string) (gocql.Consistency, error) {
	consistencyMap := map[string]gocql.Consistency{
		"ANY":          gocql.Any,
		"ONE":          gocql.One,
		"TWO":          gocql.Two,
		"THREE":        gocql.Three,
		"QUORUM":       gocql.Quorum,
		"ALL":          gocql.All,
		"LOCAL_QUORUM": gocql.LocalQuorum,
		"EACH_QUORUM":  gocql.EachQuorum,
		"LOCAL_ONE":    gocql.LocalOne,
	}

	consistency, ok := consistencyMap[strings.ToUpper(consistencyStr)]
	if !ok {
		return gocql.LocalQuorum, fmt.Errorf("unknown consistency level: %s", consistencyStr)
	}
	return consistency, nil
}
