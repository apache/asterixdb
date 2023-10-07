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
package org.apache.asterix.metadata.bootstrap;

import org.apache.asterix.metadata.api.IMetadataIndex;

public class MetadataIndexesProvider {

    protected final boolean usingDatabase;

    public MetadataIndexesProvider(boolean usingDatabase) {
        this.usingDatabase = usingDatabase;
    }

    public DatabaseEntity getDatabaseEntity() {
        return DatabaseEntity.of(usingDatabase);
    }

    public DataverseEntity getDataverseEntity() {
        return DataverseEntity.of(usingDatabase);
    }

    public DatasetEntity getDatasetEntity() {
        return DatasetEntity.of(usingDatabase);
    }

    public DatatypeEntity getDatatypeEntity() {
        return DatatypeEntity.of(usingDatabase);
    }

    public IndexEntity getIndexEntity() {
        return IndexEntity.of(usingDatabase);
    }

    public SynonymEntity getSynonymEntity() {
        return SynonymEntity.of(usingDatabase);
    }

    public NodeEntity getNodeEntity() {
        return NodeEntity.of(usingDatabase);
    }

    public NodeGroupEntity getNodeGroupEntity() {
        return NodeGroupEntity.of(usingDatabase);
    }

    public FunctionEntity getFunctionEntity() {
        return FunctionEntity.of(usingDatabase);
    }

    public DatasourceAdapterEntity getDatasourceAdapterEntity() {
        return DatasourceAdapterEntity.of(usingDatabase);
    }

    public FeedEntity getFeedEntity() {
        return FeedEntity.of(usingDatabase);
    }

    public FeedPolicyEntity getFeedPolicyEntity() {
        return FeedPolicyEntity.of(usingDatabase);
    }

    public LibraryEntity getLibraryEntity() {
        return LibraryEntity.of(usingDatabase);
    }

    public CompactionPolicyEntity getCompactionPolicyEntity() {
        return CompactionPolicyEntity.of(usingDatabase);
    }

    public ExternalFileEntity getExternalFileEntity() {
        return ExternalFileEntity.of(usingDatabase);
    }

    public FeedConnectionEntity getFeedConnectionEntity() {
        return FeedConnectionEntity.of(usingDatabase);
    }

    public FullTextConfigEntity getFullTextConfigEntity() {
        return FullTextConfigEntity.of(usingDatabase);
    }

    public FullTextFilterEntity getFullTextFilterEntity() {
        return FullTextFilterEntity.of(usingDatabase);
    }

    public IMetadataIndex[] getMetadataIndexes() {
        if (isUsingDatabase()) {
            return new IMetadataIndex[] { getDatabaseEntity().getIndex(), getDataverseEntity().getIndex(),
                    getDatasetEntity().getIndex(), getDatatypeEntity().getIndex(), getIndexEntity().getIndex(),
                    getSynonymEntity().getIndex(), getNodeEntity().getIndex(), getNodeGroupEntity().getIndex(),
                    getFunctionEntity().getIndex(), getDatasourceAdapterEntity().getIndex(), getFeedEntity().getIndex(),
                    getFeedPolicyEntity().getIndex(), getLibraryEntity().getIndex(),
                    getCompactionPolicyEntity().getIndex(), getExternalFileEntity().getIndex(),
                    getFeedConnectionEntity().getIndex(), getFullTextConfigEntity().getIndex(),
                    getFullTextFilterEntity().getIndex() };
        } else {
            return new IMetadataIndex[] { getDataverseEntity().getIndex(), getDatasetEntity().getIndex(),
                    getDatatypeEntity().getIndex(), getIndexEntity().getIndex(), getSynonymEntity().getIndex(),
                    getNodeEntity().getIndex(), getNodeGroupEntity().getIndex(), getFunctionEntity().getIndex(),
                    getDatasourceAdapterEntity().getIndex(), getFeedEntity().getIndex(),
                    getFeedPolicyEntity().getIndex(), getLibraryEntity().getIndex(),
                    getCompactionPolicyEntity().getIndex(), getExternalFileEntity().getIndex(),
                    getFeedConnectionEntity().getIndex(), getFullTextConfigEntity().getIndex(),
                    getFullTextFilterEntity().getIndex() };
        }
    }

    public boolean isUsingDatabase() {
        return usingDatabase;
    }
}
