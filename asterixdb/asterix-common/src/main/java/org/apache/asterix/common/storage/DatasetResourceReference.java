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
import java.util.Objects;

import org.apache.asterix.common.dataflow.DatasetLocalResource;
import org.apache.asterix.common.utils.StorageConstants;
import org.apache.hyracks.storage.common.LocalResource;

@SuppressWarnings("squid:S2160") // don't override equals
public class DatasetResourceReference extends ResourceReference {

    private final int datasetId;
    private final int partitionId;
    private final long resourceId;

    private DatasetResourceReference(LocalResource localResource) {
        super(Paths.get(localResource.getPath(), StorageConstants.METADATA_FILE_NAME).toString());
        final DatasetLocalResource dsResource = (DatasetLocalResource) localResource.getResource();
        datasetId = dsResource.getDatasetId();
        partitionId = dsResource.getPartition();
        resourceId = localResource.getId();
    }

    public static DatasetResourceReference of(LocalResource localResource) {
        Objects.requireNonNull(localResource);
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
        return new DatasetResourceReference(localResource);
    }
}
