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

import java.sql.ParameterMetaData;
import java.sql.Types;
import java.util.Objects;

final class ADBParameterMetaData extends ADBWrapperSupport implements ParameterMetaData {

    final ADBPreparedStatement statement;

    final int parameterCount;

    public ADBParameterMetaData(ADBPreparedStatement statement, int parameterCount) {
        this.statement = Objects.requireNonNull(statement);
        this.parameterCount = parameterCount;
    }

    @Override
    public int getParameterCount() {
        return parameterCount;
    }

    @Override
    public int getParameterMode(int parameterIndex) {
        return parameterModeIn;
    }

    @Override
    public int getParameterType(int parameterIndex) {
        return Types.OTHER; // any
    }

    @Override
    public String getParameterTypeName(int parameterIndex) {
        return "";
    }

    @Override
    public String getParameterClassName(int parameterIndex) {
        return Object.class.getName();
    }

    @Override
    public int isNullable(int parameterIndex) {
        return parameterNullable;
    }

    @Override
    public boolean isSigned(int parameterIndex) {
        return false;
    }

    @Override
    public int getPrecision(int parameterIndex) {
        return 0;
    }

    @Override
    public int getScale(int parameterIndex) {
        return 0;
    }

    @Override
    protected ADBErrorReporter getErrorReporter() {
        return statement.getErrorReporter();
    }
}
