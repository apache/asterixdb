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
package org.apache.asterix.external.dataset.adapter;

import java.io.Serializable;
import java.util.Objects;

import org.apache.asterix.common.metadata.DataverseName;

/**
 * A unique identifier for a data source adapter.
 */
public class AdapterIdentifier implements Serializable {

    private static final long serialVersionUID = 3L;

    private final String databaseName;
    private final DataverseName dataverseName;
    private final String adapterName;

    public AdapterIdentifier(String databaseName, DataverseName dataverse, String name) {
        this.databaseName = Objects.requireNonNull(databaseName);
        this.dataverseName = dataverse;
        this.adapterName = name;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public DataverseName getDataverseName() {
        return dataverseName;
    }

    public String getName() {
        return adapterName;
    }

    @Override
    public int hashCode() {
        return Objects.hash(databaseName, dataverseName + "@" + adapterName);

    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        if (this == o) {
            return true;
        }
        if (!(o instanceof AdapterIdentifier)) {
            return false;
        }
        AdapterIdentifier a = (AdapterIdentifier) o;
        return Objects.equals(databaseName, a.databaseName) && dataverseName.equals(a.dataverseName)
                && adapterName.equals(a.adapterName);
    }
}
