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

public class MetadataIndexImmutableProperties {

    public static final int FIRST_AVAILABLE_EXTENSION_METADATA_DATASET_ID = 52;
    public static final int FIRST_AVAILABLE_USER_DATASET_ID = 100;
    public static final int METADATA_DATASETS_PARTITIONS = 1;
    public static final int METADATA_DATASETS_COUNT = 14;

    private final String indexName;
    private final int datasetId;
    private final long resourceId;

    public MetadataIndexImmutableProperties(String indexName, int datasetId, long resourceId) {
        this.indexName = indexName;
        this.datasetId = datasetId;
        this.resourceId = resourceId;
    }

    public long getResourceId() {
        return resourceId;
    }

    public String getIndexName() {
        return indexName;
    }

    public int getDatasetId() {
        return datasetId;
    }

    // Right now, we only have primary Metadata indexes. Hence, dataset name is always index name
    public String getDatasetName() {
        return indexName;
    }

    public static boolean isMetadataDataset(int datasetId) {
        return datasetId >= 0 && datasetId < FIRST_AVAILABLE_USER_DATASET_ID;
    }
}
