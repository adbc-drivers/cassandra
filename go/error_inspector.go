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
	"strings"

	"github.com/apache/arrow-adbc/go/adbc"
	gocql "github.com/apache/cassandra-gocql-driver/v2"
)

type CassandraErrorInspector struct{}

// InspectError maps errors from the Cassandra client to ADBC errors.
func (CassandraErrorInspector) InspectError(err error, defaultStatus adbc.Status) adbc.Error {
	status := defaultStatus
	vendorCode := int32(0)
	sqlState := [5]byte{}

	if requestErr, ok := errors.AsType[gocql.RequestError](err); ok {
		vendorCode = int32(requestErr.Code())
		message := strings.ToLower(requestErr.Message())
		switch {
		case requestErr.Code() == gocql.ErrCodeAlreadyExists,
			requestErr.Code() == gocql.ErrCodeInvalid && strings.Contains(message, "undefined column name"):
			status = adbc.StatusAlreadyExists
		case requestErr.Code() == gocql.ErrCodeInvalid && isTableNotFoundMessage(message):
			status = adbc.StatusNotFound
			sqlState = [5]byte{'4', '2', 'S', '0', '2'}
		}
	}

	return adbc.Error{
		Code:       status,
		Msg:        err.Error(),
		VendorCode: vendorCode,
		SqlState:   sqlState,
	}
}

func isTableNotFoundMessage(message string) bool {
	message = strings.ToLower(message)
	return strings.Contains(message, "unconfigured table") ||
		(strings.Contains(message, "table") &&
			(strings.Contains(message, "does not exist") || strings.Contains(message, "doesn't exist")))
}
