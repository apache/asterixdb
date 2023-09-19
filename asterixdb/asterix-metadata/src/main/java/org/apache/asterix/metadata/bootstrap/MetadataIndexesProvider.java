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

import static org.apache.hyracks.control.common.controllers.ControllerConfig.Option.CLOUD_DEPLOYMENT;

import org.apache.asterix.metadata.api.IMetadataIndex;
import org.apache.hyracks.api.application.INCServiceContext;

public class MetadataIndexesProvider {

    protected final boolean cloudDeployment;

    public MetadataIndexesProvider(INCServiceContext ncServiceCtx) {
        cloudDeployment = ncServiceCtx.getAppConfig().getBoolean(CLOUD_DEPLOYMENT);
    }

    public DatabaseEntity getDatabaseEntity() {
        return DatabaseEntity.of(cloudDeployment);
    }

    public DataverseEntity getDataverseEntity() {
        return DataverseEntity.of(cloudDeployment);
    }

    public DatasetEntity getDatasetEntity() {
        return DatasetEntity.of(cloudDeployment);
    }

    public DatatypeEntity getDatatypeEntity() {
        return DatatypeEntity.of(cloudDeployment);
    }

    public IndexEntity getIndexEntity() {
        return IndexEntity.of(cloudDeployment);
    }

    public SynonymEntity getSynonymEntity() {
        return SynonymEntity.of(cloudDeployment);
    }

    public NodeEntity getNodeEntity() {
        return NodeEntity.of(cloudDeployment);
    }

    public NodeGroupEntity getNodeGroupEntity() {
        return NodeGroupEntity.of(cloudDeployment);
    }

    public FunctionEntity getFunctionEntity() {
        return FunctionEntity.of(cloudDeployment);
    }

    public DatasourceAdapterEntity getDatasourceAdapterEntity() {
        return DatasourceAdapterEntity.of(cloudDeployment);
    }

    public FeedEntity getFeedEntity() {
        return FeedEntity.of(cloudDeployment);
    }

    public FeedPolicyEntity getFeedPolicyEntity() {
        return FeedPolicyEntity.of(cloudDeployment);
    }

    public LibraryEntity getLibraryEntity() {
        return LibraryEntity.of(cloudDeployment);
    }

    public CompactionPolicyEntity getCompactionPolicyEntity() {
        return CompactionPolicyEntity.of(cloudDeployment);
    }

    public ExternalFileEntity getExternalFileEntity() {
        return ExternalFileEntity.of(cloudDeployment);
    }

    public FeedConnectionEntity getFeedConnectionEntity() {
        return FeedConnectionEntity.of(cloudDeployment);
    }

    public FullTextConfigEntity getFullTextConfigEntity() {
        return FullTextConfigEntity.of(cloudDeployment);
    }

    public FullTextFilterEntity getFullTextFilterEntity() {
        return FullTextFilterEntity.of(cloudDeployment);
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
        return false;
    }
}
