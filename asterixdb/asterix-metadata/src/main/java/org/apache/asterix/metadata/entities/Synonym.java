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

package org.apache.asterix.metadata.entities;

import java.util.Objects;

import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.metadata.MetadataCache;
import org.apache.asterix.metadata.api.IMetadataEntity;

public class Synonym implements IMetadataEntity<Synonym> {

    private static final long serialVersionUID = 2L;

    private final String databaseName;

    private final DataverseName dataverseName;

    private final String synonymName;

    private final String objectDatabaseName;

    private final DataverseName objectDataverseName;

    private final String objectName;

    public Synonym(String databaseName, DataverseName dataverseName, String synonymName, String objectDatabaseName,
            DataverseName objectDataverseName, String objectName) {
        this.databaseName = Objects.requireNonNull(databaseName);
        this.dataverseName = Objects.requireNonNull(dataverseName);
        this.synonymName = Objects.requireNonNull(synonymName);
        this.objectDatabaseName = Objects.requireNonNull(objectDatabaseName);
        this.objectDataverseName = Objects.requireNonNull(objectDataverseName);
        this.objectName = Objects.requireNonNull(objectName);
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public DataverseName getDataverseName() {
        return dataverseName;
    }

    public String getSynonymName() {
        return synonymName;
    }

    public String getObjectDatabaseName() {
        return objectDatabaseName;
    }

    public DataverseName getObjectDataverseName() {
        return objectDataverseName;
    }

    public String getObjectName() {
        return objectName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        Synonym synonym = (Synonym) o;
        return Objects.equals(databaseName, synonym.databaseName) && dataverseName.equals(synonym.dataverseName)
                && synonymName.equals(synonym.synonymName)
                && Objects.equals(objectDatabaseName, synonym.objectDatabaseName)
                && objectDataverseName.equals(synonym.objectDataverseName) && objectName.equals(synonym.objectName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(databaseName, dataverseName, synonymName, objectDatabaseName, objectDataverseName,
                objectName);
    }

    @Override
    public Synonym addToCache(MetadataCache cache) {
        return cache.addSynonymIfNotExists(this);
    }

    @Override
    public Synonym dropFromCache(MetadataCache cache) {
        return cache.dropSynonym(this);
    }
}
