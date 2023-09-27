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

package org.apache.asterix.metadata.declared;

import java.util.Arrays;
import java.util.Objects;

import org.apache.asterix.common.metadata.DataverseName;

public final class DataSourceId {

    private final String databaseName;

    private final DataverseName dataverseName;

    private final String datasourceName;

    private final String[] parameters;

    /**
     * The original constructor taking
     *
     * @param databaseName
     *         the database name
     * @param dataverseName
     *         the dataverse (namespace) for this datasource
     * @param datasourceName
     *         the name for this datasource
     */
    public DataSourceId(String databaseName, DataverseName dataverseName, String datasourceName) {
        this(databaseName, dataverseName, datasourceName, null);
    }

    /**
     * An extended constructor taking an arbitrary number of name parameters.
     * This constructor allows the definition of datasources that have the same dataverse name and datasource name but
     * that would expose different behavior. It enables the definition of (compile-time) parameterized datasources.
     * Please note that the first 2 parameters still need to be 1) a dataverse name and 2) a datasource name.
     */
    public DataSourceId(String databaseName, DataverseName dataverseName, String datasourceName, String[] parameters) {
        this.databaseName = databaseName;
        this.dataverseName = dataverseName;
        this.datasourceName = datasourceName;
        this.parameters = parameters;
    }

    @Override
    public String toString() {
        return dataverseName + "." + datasourceName + (parameters != null ? "." + String.join(".", parameters) : "");
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public DataverseName getDataverseName() {
        return dataverseName;
    }

    public String getDatasourceName() {
        return datasourceName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DataSourceId that = (DataSourceId) o;
        return Objects.equals(databaseName, that.databaseName) && dataverseName.equals(that.dataverseName)
                && datasourceName.equals(that.datasourceName) && Arrays.equals(parameters, that.parameters);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(databaseName, dataverseName, datasourceName);
        result = 31 * result + Arrays.hashCode(parameters);
        return result;
    }
}
