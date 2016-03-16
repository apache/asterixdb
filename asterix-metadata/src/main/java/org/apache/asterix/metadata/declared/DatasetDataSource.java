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

import java.util.List;

import org.apache.asterix.metadata.IDatasetDetails;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.InternalDatasetDetails;
import org.apache.asterix.metadata.utils.KeyFieldTypeUtils;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.properties.INodeDomain;

public class DatasetDataSource extends AqlDataSource {

    private Dataset dataset;

    public DatasetDataSource(AqlSourceId id, Dataset dataset, IAType itemType, IAType metaItemType,
            AqlDataSourceType datasourceType, IDatasetDetails datasetDetails, INodeDomain datasetDomain)
                    throws AlgebricksException {
        super(id, itemType, metaItemType, datasourceType, datasetDomain);
        this.dataset = dataset;
        switch (dataset.getDatasetType()) {
            case INTERNAL:
                initInternalDataset(itemType, metaItemType, datasetDetails);
                break;
            case EXTERNAL:
                initExternalDataset(itemType);
                break;
        }
    }

    public Dataset getDataset() {
        return dataset;
    }

    private void initInternalDataset(IAType itemType, IAType metaItemType, IDatasetDetails datasetDetails)
            throws AlgebricksException {
        InternalDatasetDetails internalDatasetDetails = (InternalDatasetDetails) datasetDetails;
        ARecordType recordType = (ARecordType) itemType;
        ARecordType metaRecordType = (ARecordType) metaItemType;
        List<IAType> partitioningKeyTypes = KeyFieldTypeUtils.getPartitioningKeyTypes(internalDatasetDetails,
                recordType, metaRecordType);
        int n = partitioningKeyTypes.size();
        schemaTypes = metaItemType == null ? new IAType[n + 1] : new IAType[n + 2];
        for (int keyIndex = 0; keyIndex < n; ++keyIndex) {
            schemaTypes[keyIndex] = partitioningKeyTypes.get(keyIndex);
        }
        schemaTypes[n] = itemType;
        if (metaItemType != null) {
            schemaTypes[n + 1] = metaItemType;
        }
    }

    private void initExternalDataset(IAType itemType) {
        schemaTypes = new IAType[1];
        schemaTypes[0] = itemType;
    }

}
