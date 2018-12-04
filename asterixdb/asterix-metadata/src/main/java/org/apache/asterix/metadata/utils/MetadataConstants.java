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

package org.apache.asterix.metadata.utils;

/**
 * Contains metadata constants
 */
public class MetadataConstants {

    // Name of the dataverse the metadata lives in.
    public static final String METADATA_DATAVERSE_NAME = "Metadata";
    // Name of the node group where metadata is stored on.
    public static final String METADATA_NODEGROUP_NAME = "MetadataGroup";

    public static final String DATAVERSE_DATASET_NAME = "Dataverse";
    public static final String DATASET_DATASET_NAME = "Dataset";
    public static final String INDEX_DATASET_NAME = "Index";
    public static final String DATATYPE_DATASET_NAME = "Datatype";
    public static final String NODE_DATASET_NAME = "Node";
    public static final String NODEGROUP_DATASET_NAME = "Nodegroup";
    public static final String FUNCTION_DATASET_NAME = "Function";
    public static final String DATASOURCE_ADAPTER_DATASET_NAME = "DatasourceAdapter";
    public static final String LIBRARY_DATASET_NAME = "Library";
    public static final String FEED_DATASET_NAME = "Feed";
    public static final String FEED_CONNECTION_DATASET_NAME = "FeedConnection";
    public static final String FEED_POLICY_DATASET_NAME = "FeedPolicy";
    public static final String COMPACTION_POLICY_DATASET_NAME = "CompactionPolicy";
    public static final String EXTERNAL_FILE_DATASET_NAME = "ExternalFile";

    private MetadataConstants() {
    }
}
