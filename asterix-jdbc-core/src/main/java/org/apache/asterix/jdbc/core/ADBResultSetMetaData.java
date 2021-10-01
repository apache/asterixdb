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

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

final class ADBResultSetMetaData extends ADBWrapperSupport implements ResultSetMetaData {

    final ADBStatement statement;

    private final List<ADBColumn> columns;

    private final Map<String, Integer> indexByName;

    ADBResultSetMetaData(ADBStatement statement, List<ADBColumn> columns) {
        this.statement = Objects.requireNonNull(statement);
        this.columns = columns != null ? columns : Collections.emptyList();
        this.indexByName = createIndexByName(this.columns);
    }

    @Override
    public int getColumnCount() {
        return columns.size();
    }

    @Override
    public String getColumnName(int columnNumber) throws SQLException {
        return getColumnByNumber(columnNumber).getName();
    }

    @Override
    public String getColumnLabel(int columnNumber) throws SQLException {
        return getColumnByNumber(columnNumber).getName();
    }

    @Override
    public int getColumnType(int columnNumber) throws SQLException {
        return getColumnByNumber(columnNumber).getType().getJdbcType().getVendorTypeNumber();
    }

    @Override
    public String getColumnTypeName(int columnNumber) throws SQLException {
        return getColumnByNumber(columnNumber).getType().getTypeName();
    }

    @Override
    public String getColumnClassName(int columnNumber) throws SQLException {
        return getColumnByNumber(columnNumber).getType().getJavaClass().getName();
    }

    @Override
    public int getColumnDisplaySize(int columnNumber) {
        // TODO:based on type
        return 1;
    }

    @Override
    public int getPrecision(int columnNumber) {
        // TODO:based on type
        return 0;
    }

    @Override
    public int getScale(int columnNumber) {
        return 0;
    }

    @Override
    public boolean isAutoIncrement(int columnNumber) {
        return false;
    }

    @Override
    public boolean isCaseSensitive(int columnNumber) {
        return false;
    }

    @Override
    public boolean isCurrency(int columnNumber) {
        return false;
    }

    @Override
    public int isNullable(int columnNumber) throws SQLException {
        return getColumnByNumber(columnNumber).isOptional() ? columnNullable : columnNoNulls;
    }

    @Override
    public boolean isSearchable(int columnNumber) {
        return true;
    }

    @Override
    public boolean isSigned(int columnNumber) {
        return false;
    }

    @Override
    public boolean isReadOnly(int columnNumber) {
        return false;
    }

    @Override
    public boolean isWritable(int columnNumber) {
        return false;
    }

    @Override
    public boolean isDefinitelyWritable(int columnNumber) {
        return false;
    }

    @Override
    public String getCatalogName(int columnNumber) {
        return "";
    }

    @Override
    public String getSchemaName(int columnNumber) {
        return "";
    }

    @Override
    public String getTableName(int columnNumber) {
        return "";
    }

    // Helpers

    private ADBColumn getColumnByNumber(int columnNumber) throws SQLException {
        return getColumnByIndex(toColumnIndex(columnNumber));
    }

    private int toColumnIndex(int columnNumber) throws SQLException {
        boolean ok = 0 < columnNumber && columnNumber <= columns.size();
        if (!ok) {
            throw getErrorReporter().errorParameterValueNotSupported("columnNumber");
        }
        return columnNumber - 1;
    }

    ADBColumn getColumnByIndex(int idx) {
        return columns.get(idx);
    }

    int findColumnIndexByName(String columnName) {
        Integer idx = indexByName.get(columnName);
        return idx != null ? idx : -1;
    }

    private static Map<String, Integer> createIndexByName(List<ADBColumn> columns) {
        int n = columns.size();
        switch (n) {
            case 0:
                return Collections.emptyMap();
            case 1:
                return Collections.singletonMap(columns.get(0).getName(), 0);
            default:
                Map<String, Integer> m = new HashMap<>();
                for (int i = 0; i < n; i++) {
                    m.put(columns.get(i).getName(), i);
                }
                return m;
        }
    }

    @Override
    protected ADBErrorReporter getErrorReporter() {
        return statement.getErrorReporter();
    }
}
