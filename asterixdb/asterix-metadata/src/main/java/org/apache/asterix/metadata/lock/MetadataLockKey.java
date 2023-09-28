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

package org.apache.asterix.metadata.lock;

import java.util.Objects;

import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.common.metadata.IMetadataLock;

final class MetadataLockKey implements IMetadataLock.LockKey {

    enum EntityKind {
        ACTIVE,
        DATASET,
        DATATYPE,
        DATABASE,
        DATAVERSE,
        EXTENSION,
        FEED_POLICY,
        FUNCTION,
        FULL_TEXT_CONFIG,
        FULL_TEXT_FILTER,
        LIBRARY,
        ADAPTER,
        MERGE_POLICY,
        NODE_GROUP,
        SYNONYM
    }

    private final EntityKind entityKind;

    private final String entityKindExtension;

    private final String database;

    private final DataverseName dataverseName;

    private final String entityName;

    private MetadataLockKey(EntityKind entityKind, String entityKindExtension, String database,
            DataverseName dataverseName, String entityName) {
        if (entityKind == null || (database == null && dataverseName == null && entityName == null)) {
            throw new NullPointerException();
        }
        this.entityKind = entityKind;
        this.entityKindExtension = entityKindExtension;
        this.database = database;
        this.dataverseName = dataverseName;
        this.entityName = entityName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MetadataLockKey that = (MetadataLockKey) o;
        return entityKind == that.entityKind && Objects.equals(entityKindExtension, that.entityKindExtension)
                && Objects.equals(database, that.database) && Objects.equals(dataverseName, that.dataverseName)
                && Objects.equals(entityName, that.entityName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(entityKind, entityKindExtension, database, dataverseName, entityName);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(64);
        sb.append(entityKind);
        if (entityKindExtension != null) {
            sb.append(':').append(entityKindExtension);
        }
        if (dataverseName != null) {
            sb.append(':').append(dataverseName.getCanonicalForm());
        }
        sb.append(':').append(entityName);
        return sb.toString();
    }

    static MetadataLockKey createDatabaseLockKey(String database) {
        return new MetadataLockKey(EntityKind.DATABASE, null, database, null, null);
    }

    static MetadataLockKey createDataverseLockKey(String database, DataverseName dataverseName) {
        return new MetadataLockKey(EntityKind.DATAVERSE, null, database, dataverseName, null);
    }

    static MetadataLockKey createDatasetLockKey(String database, DataverseName dataverseName, String datasetName) {
        return new MetadataLockKey(EntityKind.DATASET, null, database, dataverseName, datasetName);
    }

    static MetadataLockKey createDataTypeLockKey(String database, DataverseName dataverseName, String datatypeName) {
        return new MetadataLockKey(EntityKind.DATATYPE, null, database, dataverseName, datatypeName);
    }

    static MetadataLockKey createFunctionLockKey(String database, DataverseName dataverseName, String functionName) {
        return new MetadataLockKey(EntityKind.FUNCTION, null, database, dataverseName, functionName);
    }

    static MetadataLockKey createFullTextConfigLockKey(String database, DataverseName dataverseName,
            String fullTextConfigName) {
        return new MetadataLockKey(EntityKind.FULL_TEXT_CONFIG, null, database, dataverseName, fullTextConfigName);
    }

    static MetadataLockKey createFullTextFilterLockKey(String database, DataverseName dataverseName,
            String fullTextFilterName) {
        return new MetadataLockKey(EntityKind.FULL_TEXT_FILTER, null, database, dataverseName, fullTextFilterName);
    }

    static MetadataLockKey createLibraryLockKey(String database, DataverseName dataverseName, String libraryName) {
        return new MetadataLockKey(EntityKind.LIBRARY, null, database, dataverseName, libraryName);
    }

    static MetadataLockKey createAdapterLockKey(String database, DataverseName dataverseName, String adapterName) {
        return new MetadataLockKey(EntityKind.ADAPTER, null, database, dataverseName, adapterName);
    }

    static MetadataLockKey createActiveEntityLockKey(String database, DataverseName dataverseName, String entityName) {
        return new MetadataLockKey(EntityKind.ACTIVE, null, database, dataverseName, entityName);
    }

    static MetadataLockKey createFeedPolicyLockKey(String database, DataverseName dataverseName,
            String feedPolicyName) {
        return new MetadataLockKey(EntityKind.FEED_POLICY, null, database, dataverseName, feedPolicyName);
    }

    static MetadataLockKey createSynonymLockKey(String database, DataverseName dataverseName, String synonymName) {
        return new MetadataLockKey(EntityKind.SYNONYM, null, database, dataverseName, synonymName);
    }

    static MetadataLockKey createExtensionEntityLockKey(String extension, String database, DataverseName dataverseName,
            String entityName) {
        return new MetadataLockKey(EntityKind.EXTENSION, extension, database, dataverseName, entityName);
    }

    static MetadataLockKey createNodeGroupLockKey(String nodeGroupName) {
        return new MetadataLockKey(EntityKind.NODE_GROUP, null, null, null, nodeGroupName);
    }

    static MetadataLockKey createMergePolicyLockKey(String mergePolicyName) {
        return new MetadataLockKey(EntityKind.MERGE_POLICY, null, null, null, mergePolicyName);
    }
}
