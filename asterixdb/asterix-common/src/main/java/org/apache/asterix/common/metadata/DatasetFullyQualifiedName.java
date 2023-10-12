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
package org.apache.asterix.common.metadata;

import java.io.Serializable;
import java.util.Objects;

public class DatasetFullyQualifiedName implements Serializable {

    private static final long serialVersionUID = 2L;
    private final String databaseName;
    private final DataverseName dataverseName;
    private final String datasetName;

    public DatasetFullyQualifiedName(String databaseName, DataverseName dataverseName, String datasetName) {
        this.databaseName = databaseName;
        this.dataverseName = dataverseName;
        this.datasetName = datasetName;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public DataverseName getDataverseName() {
        return dataverseName;
    }

    public String getDatasetName() {
        return datasetName;
    }

    @Override
    public String toString() {
        return (MetadataConstants.DEFAULT_DATABASE.equals(databaseName) ? "" : datasetName + ".") + dataverseName + "."
                + datasetName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o instanceof DatasetFullyQualifiedName) {
            DatasetFullyQualifiedName that = (DatasetFullyQualifiedName) o;
            return Objects.equals(databaseName, that.databaseName) && dataverseName.equals(that.dataverseName)
                    && datasetName.equals(that.datasetName);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(databaseName, dataverseName, datasetName);
    }
}
