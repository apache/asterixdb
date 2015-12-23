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

public enum MetadataIndexImmutableProperties {
    METADATA(MetadataConstants.METADATA_DATAVERSE_NAME, 0, 0),
    DATAVERSE("Dataverse", 1, 1),
    DATASET("Dataset", 2, 2),
    DATATYPE("Datatype", 3, 3),
    INDEX("Index", 4, 4),
    NODE("Node", 5, 5),
    NODEGROUP("Nodegroup", 6, 6),
    FUNCTION("Function", 7, 7),
    DATASOURCE_ADAPTER("DatasourceAdapter", 8, 8),
    LIBRARY("Library", 9, 9),
    FEED("Feed", 10, 10),
    FEED_ACTIVITY_DATASET_ID("FeedActivity", 11, 11),
    FEED_POLICY("FeedPolicy", 12, 12),
    COMPACTION_POLICY("CompactionPolicy", 13, 13),
    EXTERNAL_FILE("ExternalFile", 14, 14),
    GROUPNAME_ON_DATASET("GroupName", DATASET, 15),
    DATATYPE_NAME_ON_DATASET("DatatypeName", DATASET, 16),
    DATATYPE_NAME_ON_DATATYPE("DatatypeName", DATATYPE, 17);

    private final String indexName;
    private final int datasetId;
    private final long resourceId;
    private final MetadataIndexImmutableProperties dataset;

    public static final int FIRST_AVAILABLE_USER_DATASET_ID = 100;

    private MetadataIndexImmutableProperties(String indexName, int datasetId, long resourceId) {
        this.indexName = indexName;
        this.datasetId = datasetId;
        this.resourceId = resourceId;
        //a primary index's dataset is itself
        this.dataset = this;
    }

    private MetadataIndexImmutableProperties(String indexName, MetadataIndexImmutableProperties dataset,
            long resourceId) {
        this.indexName = indexName;
        this.datasetId = dataset.datasetId;
        this.resourceId = resourceId;
        this.dataset = dataset;
    }

    public long getResourceId() {
        return resourceId;
    }

    public String getIndexName() {
        return indexName;
    }

    public String getDatasetName() {
        return dataset.indexName;
    }

    public int getDatasetId() {
        return dataset.datasetId;
    }
}