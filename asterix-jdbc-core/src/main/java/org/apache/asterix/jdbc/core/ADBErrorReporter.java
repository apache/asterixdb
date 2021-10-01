/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.asterix.jdbc.core;

import java.io.IOException;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLInvalidAuthorizationSpecException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLTimeoutException;
import java.sql.SQLTransientConnectionException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.apache.http.conn.ConnectTimeoutException;

import com.fasterxml.jackson.core.JsonProcessingException;

final class ADBErrorReporter {

    private static final List<Class<? extends IOException>> TRANSIENT_CONNECTION_ERRORS =
            Arrays.asList(java.net.NoRouteToHostException.class, org.apache.http.NoHttpResponseException.class,
                    org.apache.http.conn.HttpHostConnectException.class);

    protected SQLException errorObjectClosed(Class<?> jdbcInterface) {
        return new SQLException(String.format("%s is closed", jdbcInterface.getSimpleName()));
    }

    protected SQLFeatureNotSupportedException errorMethodNotSupported(Class<?> jdbcInterface, String methodName) {
        return new SQLFeatureNotSupportedException(
                String.format("Method %s.%s() is not supported", jdbcInterface.getName(), methodName));
    }

    protected SQLClientInfoException errorClientInfoMethodNotSupported(Class<?> jdbcInterface, String methodName) {
        return new SQLClientInfoException(
                String.format("Method %s.%s() is not supported", jdbcInterface.getName(), methodName),
                Collections.emptyMap());
    }

    protected SQLException errorParameterNotSupported(String parameterName) {
        return new SQLException(String.format("Unsupported parameter %s", parameterName));
    }

    protected String warningParameterNotSupported(String parameterName) {
        return String.format("Unsupported parameter %s", parameterName);
    }

    protected SQLException errorParameterValueNotSupported(String parameterName) {
        return new SQLException(String.format("Unsupported or invalid value of %s parameter", parameterName));
    }

    protected String warningParameterValueNotSupported(String parameterName) {
        return String.format("Ignored unsupported or invalid value of %s parameter", parameterName);
    }

    protected SQLException errorIncompatibleMode(String mode) {
        return new SQLException(String.format("Operation cannot be performed in %s mode", mode));
    }

    protected SQLException errorInProtocol() {
        return new SQLNonTransientConnectionException("Protocol error", SQLState.CONNECTION_FAILURE.code);
    }

    protected SQLException errorInProtocol(String badValue) {
        return new SQLNonTransientConnectionException(String.format("Protocol error. Unexpected %s", badValue),
                SQLState.CONNECTION_FAILURE.code);
    }

    protected SQLException errorInProtocol(JsonProcessingException e) {
        return new SQLNonTransientConnectionException(String.format("Protocol error. %s", getMessage(e)),
                SQLState.CONNECTION_FAILURE.code, e);
    }

    protected SQLException errorInConnection(String badValue) {
        return new SQLNonTransientConnectionException(String.format("Connection error. Unexpected %s", badValue),
                SQLState.CONNECTION_FAILURE.code);
    }

    protected SQLException errorInConnection(IOException e) {
        String message = String.format("Connection error. %s", getMessage(e));
        return e instanceof ConnectTimeoutException ? errorTimeout(message, e)
                : couldBeTransientConnectionError(e)
                        ? new SQLTransientConnectionException(message, SQLState.CONNECTION_FAILURE.code, e)
                        : new SQLNonTransientConnectionException(message, SQLState.CONNECTION_FAILURE.code, e);
    }

    protected SQLException errorClosingResource(IOException e) {
        return new SQLException(String.format("Error closing resources. %s", getMessage(e)), e);
    }

    protected SQLInvalidAuthorizationSpecException errorAuth() {
        return new SQLInvalidAuthorizationSpecException("Authentication/authorization error",
                SQLState.CONNECTION_REJECTED.code);
    }

    protected SQLException errorColumnNotFound(String columnNameOrNumber) {
        return new SQLException(String.format("Column %s was not found", columnNameOrNumber));
    }

    protected SQLException errorUnexpectedColumnValue(ADBDatatype type, String columnName) {
        return new SQLException(
                String.format("Unexpected value of type %s for column %s", type.getTypeName(), columnName));
    }

    protected SQLException errorUnwrapTypeMismatch(Class<?> iface) {
        return new SQLException(String.format("Cannot unwrap to %s", iface.getName()));
    }

    protected SQLException errorInvalidStatementCategory() {
        return new SQLException("Invalid statement category");
    }

    protected SQLException errorUnexpectedType(Class<?> type) {
        return new SQLException(String.format("Unexpected type %s", type.getName()), SQLState.INVALID_DATE_TYPE.code);
    }

    protected SQLException errorUnexpectedType(byte typeTag) {
        return new SQLException(String.format("Unexpected type %s", typeTag), SQLState.INVALID_DATE_TYPE.code);
    }

    protected SQLException errorUnexpectedType(ADBDatatype type) {
        return new SQLException(String.format("Unexpected type %s", type.getTypeName()),
                SQLState.INVALID_DATE_TYPE.code);
    }

    protected SQLException errorInvalidValueOfType(ADBDatatype type) {
        return new SQLException(String.format("Invalid value of type %s", type), SQLState.INVALID_DATE_TYPE.code);
    }

    protected SQLException errorNoResult() {
        return new SQLException("Result is unavailable");
    }

    protected SQLException errorBadResultSignature() {
        return new SQLException("Cannot infer result columns");
    }

    protected SQLException errorNoCurrentRow() {
        return new SQLException("No current row", SQLState.INVALID_CURSOR_POSITION.code);
    }

    protected SQLException errorInRequestGeneration(IOException e) {
        return new SQLException(String.format("Cannot create request. %s", getMessage(e)), e);
    }

    protected SQLException errorInResultHandling(IOException e) {
        return new SQLException(String.format("Cannot reading result. %s", getMessage(e)), e);
    }

    protected SQLTimeoutException errorTimeout() {
        return new SQLTimeoutException();
    }

    protected SQLTimeoutException errorTimeout(String message, IOException cause) {
        return new SQLTimeoutException(message, cause);
    }

    protected boolean couldBeTransientConnectionError(IOException e) {
        if (e != null) {
            for (Class<? extends IOException> c : TRANSIENT_CONNECTION_ERRORS) {
                if (c.isInstance(e)) {
                    return true;
                }
            }

        }
        return false;
    }

    protected String getMessage(Exception e) {
        String message = e != null ? e.getMessage() : null;
        return message != null ? message : "";
    }

    public enum SQLState {
        CONNECTION_REJECTED("08004"),
        CONNECTION_FAILURE("08006"),
        INVALID_DATE_TYPE("HY004"),
        INVALID_CURSOR_POSITION("HY108");

        private final String code;

        SQLState(String code) {
            this.code = Objects.requireNonNull(code);
        }
    }
}
