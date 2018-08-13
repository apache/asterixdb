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
package org.apache.asterix.common.storage;

import java.nio.file.Paths;

import org.apache.asterix.common.dataflow.DatasetLocalResource;
import org.apache.asterix.common.utils.StorageConstants;
import org.apache.hyracks.storage.common.LocalResource;

public class DatasetResourceReference extends ResourceReference {

    private int datasetId;
    private int partitionId;
    private long resourceId;

    private DatasetResourceReference() {
        super();
    }

    public static DatasetResourceReference of(LocalResource localResource) {
        return parse(localResource);
    }

    public int getDatasetId() {
        return datasetId;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public long getResourceId() {
        return resourceId;
    }

    private static DatasetResourceReference parse(LocalResource localResource) {
        final DatasetResourceReference datasetResourceReference = new DatasetResourceReference();
        final String filePath = Paths.get(localResource.getPath(), StorageConstants.METADATA_FILE_NAME).toString();
        parse(datasetResourceReference, filePath);
        assignIds(localResource, datasetResourceReference);
        return datasetResourceReference;
    }

    private static void assignIds(LocalResource localResource, DatasetResourceReference lrr) {
        final DatasetLocalResource dsResource = (DatasetLocalResource) localResource.getResource();
        lrr.datasetId = dsResource.getDatasetId();
        lrr.partitionId = dsResource.getPartition();
        lrr.resourceId = localResource.getId();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o instanceof ResourceReference) {
            ResourceReference that = (ResourceReference) o;
            return getRelativePath().toString().equals(that.getRelativePath().toString());
        }
        return false;
    }

    @Override
    public int hashCode() {
        return getRelativePath().toString().hashCode();
    }

    @Override
    public String toString() {
        return getRelativePath().toString();
    }
}
