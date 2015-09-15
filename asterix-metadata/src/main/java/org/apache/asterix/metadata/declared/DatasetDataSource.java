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

import java.io.IOException;
import java.util.List;

import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.utils.DatasetUtils;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.properties.DefaultNodeGroupDomain;
import org.apache.hyracks.algebricks.core.algebra.properties.ILocalStructuralProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.INodeDomain;

public class DatasetDataSource extends AqlDataSource {

    private Dataset dataset;

    public DatasetDataSource(AqlSourceId id, String datasourceDataverse, String datasourceName, IAType itemType,
            AqlDataSourceType datasourceType) throws AlgebricksException {
        super(id, datasourceDataverse, datasourceName, itemType, datasourceType);
        MetadataTransactionContext ctx = null;
        try {
            ctx = MetadataManager.INSTANCE.beginTransaction();
            dataset = MetadataManager.INSTANCE.getDataset(ctx, datasourceDataverse, datasourceName);
            if (dataset == null) {
                throw new AlgebricksException("Unknown dataset " + datasourceName + " in dataverse "
                        + datasourceDataverse);
            }
            MetadataManager.INSTANCE.commitTransaction(ctx);
            switch (dataset.getDatasetType()) {
                case INTERNAL:
                    initInternalDataset(itemType);
                    break;
                case EXTERNAL:
                    initExternalDataset(itemType);
                    break;

            }
        } catch (Exception e) {
            if (ctx != null) {
                try {
                    MetadataManager.INSTANCE.abortTransaction(ctx);
                } catch (Exception e2) {
                    e2.addSuppressed(e);
                    throw new IllegalStateException("Unable to abort " + e2.getMessage());
                }
            }

        }

    }

    public Dataset getDataset() {
        return dataset;
    }

    private void initInternalDataset(IAType itemType) throws IOException, AlgebricksException {
        List<List<String>> partitioningKeys = DatasetUtils.getPartitioningKeys(dataset);
        ARecordType recordType = (ARecordType) itemType;
        int n = partitioningKeys.size();
        schemaTypes = new IAType[n + 1];
        for (int i = 0; i < n; i++) {
            schemaTypes[i] = recordType.getSubFieldType(partitioningKeys.get(i));
        }
        schemaTypes[n] = itemType;
        domain = new DefaultNodeGroupDomain(dataset.getNodeGroupName());
    }

    private void initExternalDataset(IAType itemType) {
        schemaTypes = new IAType[1];
        schemaTypes[0] = itemType;
        INodeDomain domainForExternalData = new INodeDomain() {
            @Override
            public Integer cardinality() {
                return null;
            }

            @Override
            public boolean sameAs(INodeDomain domain) {
                return domain == this;
            }
        };
        domain = domainForExternalData;
    }

    @Override
    public IAType[] getSchemaTypes() {
        return schemaTypes;
    }

    @Override
    public INodeDomain getDomain() {
        return domain;
    }

    @Override
    public void computeLocalStructuralProperties(List<ILocalStructuralProperty> localProps,
            List<LogicalVariable> variables) {
        // do nothing
    }

}
