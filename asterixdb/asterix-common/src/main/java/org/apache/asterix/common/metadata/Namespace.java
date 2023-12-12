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

public final class Namespace implements Serializable, Comparable<Namespace> {

    private static final long serialVersionUID = 1L;

    private final String databaseName;
    private final DataverseName dataverseName;

    public Namespace(String databaseName, DataverseName dataverseName) {
        this.databaseName = Objects.requireNonNull(databaseName);
        this.dataverseName = Objects.requireNonNull(dataverseName);
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public DataverseName getDataverseName() {
        return dataverseName;
    }

    @Override
    public String toString() {
        return databaseName + "." + dataverseName;
    }

    @Override
    public int hashCode() {
        return Objects.hash(databaseName, dataverseName);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Namespace)) {
            return false;
        }
        Namespace that = (Namespace) obj;
        return Objects.equals(databaseName, that.databaseName) && Objects.equals(dataverseName, that.dataverseName);
    }

    @Override
    public int compareTo(Namespace that) {
        int dbComp = databaseName.compareTo(that.getDatabaseName());
        if (dbComp == 0) {
            return dataverseName.compareTo(that.getDataverseName());
        }
        return dbComp;
    }
}
