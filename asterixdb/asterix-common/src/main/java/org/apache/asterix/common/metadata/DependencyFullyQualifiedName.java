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

public class DependencyFullyQualifiedName implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String databaseName;
    private final DataverseName dataverseName;
    private final String subName1;
    private final String subName2;

    public DependencyFullyQualifiedName(String databaseName, DataverseName dataverseName, String subName1,
            String subName2) {
        this.databaseName = databaseName;
        this.dataverseName = dataverseName;
        this.subName1 = subName1;
        this.subName2 = subName2;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public DataverseName getDataverseName() {
        return dataverseName;
    }

    public String getSubName1() {
        return subName1;
    }

    public String getSubName2() {
        return subName2;
    }

    @Override
    public String toString() {
        return databaseName + "." + dataverseName + "." + subName1 + "." + subName2;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o instanceof DependencyFullyQualifiedName) {
            DependencyFullyQualifiedName that = (DependencyFullyQualifiedName) o;
            return Objects.equals(databaseName, that.databaseName) && Objects.equals(dataverseName, that.dataverseName)
                    && Objects.equals(subName1, that.subName1) && Objects.equals(subName2, that.subName2);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(databaseName, dataverseName, subName1, subName2);
    }
}
