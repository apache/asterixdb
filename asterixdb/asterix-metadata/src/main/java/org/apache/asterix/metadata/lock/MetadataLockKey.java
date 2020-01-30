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
        DATAVERSE,
        EXTENSION,
        FEED_POLICY,
        FUNCTION,
        LIBRARY,
        ADAPTER,
        MERGE_POLICY,
        NODE_GROUP,
        SYNONYM
    }

    private final EntityKind entityKind;

    private final String entityKindExtension;

    private final DataverseName dataverseName;

    private final String entityName;

    private MetadataLockKey(EntityKind entityKind, String entityKindExtension, DataverseName dataverseName,
            String entityName) {
        if (entityKind == null || (dataverseName == null && entityName == null)) {
            throw new NullPointerException();
        }
        this.entityKind = entityKind;
        this.entityKindExtension = entityKindExtension;
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
                && Objects.equals(dataverseName, that.dataverseName) && Objects.equals(entityName, that.entityName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(entityKind, entityKindExtension, dataverseName, entityName);
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

    static MetadataLockKey createDataverseLockKey(DataverseName dataverseName) {
        return new MetadataLockKey(EntityKind.DATAVERSE, null, dataverseName, null);
    }

    static MetadataLockKey createDatasetLockKey(DataverseName dataverseName, String datasetName) {
        return new MetadataLockKey(EntityKind.DATASET, null, dataverseName, datasetName);
    }

    static MetadataLockKey createDataTypeLockKey(DataverseName dataverseName, String datatypeName) {
        return new MetadataLockKey(EntityKind.DATATYPE, null, dataverseName, datatypeName);
    }

    static MetadataLockKey createFunctionLockKey(DataverseName dataverseName, String functionName) {
        return new MetadataLockKey(EntityKind.FUNCTION, null, dataverseName, functionName);
    }

    static MetadataLockKey createLibraryLockKey(DataverseName dataverseName, String libraryName) {
        return new MetadataLockKey(EntityKind.LIBRARY, null, dataverseName, libraryName);
    }

    static MetadataLockKey createAdapterLockKey(DataverseName dataverseName, String adapterName) {
        return new MetadataLockKey(EntityKind.ADAPTER, null, dataverseName, adapterName);
    }

    static MetadataLockKey createActiveEntityLockKey(DataverseName dataverseName, String entityName) {
        return new MetadataLockKey(EntityKind.ACTIVE, null, dataverseName, entityName);
    }

    static MetadataLockKey createFeedPolicyLockKey(DataverseName dataverseName, String feedPolicyName) {
        return new MetadataLockKey(EntityKind.FEED_POLICY, null, dataverseName, feedPolicyName);
    }

    static MetadataLockKey createSynonymLockKey(DataverseName dataverseName, String synonymName) {
        return new MetadataLockKey(EntityKind.SYNONYM, null, dataverseName, synonymName);
    }

    static MetadataLockKey createExtensionEntityLockKey(String extension, DataverseName dataverseName,
            String entityName) {
        return new MetadataLockKey(EntityKind.EXTENSION, extension, dataverseName, entityName);
    }

    static MetadataLockKey createNodeGroupLockKey(String nodeGroupName) {
        return new MetadataLockKey(EntityKind.NODE_GROUP, null, null, nodeGroupName);
    }

    static MetadataLockKey createMergePolicyLockKey(String mergePolicyName) {
        return new MetadataLockKey(EntityKind.MERGE_POLICY, null, null, mergePolicyName);
    }
}