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

public final class DataSourceId {

    private String[] components;

    /**
     * The original constructor taking
     *
     * @param dataverseName
     *            the dataverse (namespace) for this datasource
     * @param datasourceName
     *            the name for this datasource
     */
    public DataSourceId(String dataverseName, String datasourceName) {
        this(new String[] { dataverseName, datasourceName });
    }

    /**
     * An extended constructor taking an arbitrary number of name components.
     * This constructor allows the definition of datasources that have the same dataverse name and datasource name but
     * that would expose different behavior. It enables the definition of (compile-time) parameterized datasources.
     * Please note that the first 2 parameters still need to be 1) a dataverse name and 2) a datasource name.
     *
     * @param components
     *            name components used to construct the datasource identifier.
     */
    public DataSourceId(String... components) {
        this.components = components;
    }

    @Override
    public String toString() {
        return String.join(".", components);
    }

    public String getDataverseName() {
        return components[0];
    }

    public String getDatasourceName() {
        return components[1];
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        return Arrays.equals(components, ((DataSourceId) o).components);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(components);
    }
}
