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

import java.util.regex.Pattern;

import org.apache.commons.lang3.SystemUtils;

/**
 * Contains metadata constants
 */
public class MetadataConstants {

    public static final int DB_SCOPE_PARTS_COUNT = 1;
    public static final int METADATA_OBJECT_NAME_LENGTH_LIMIT_UTF8 = 251;
    public static final int DATAVERSE_NAME_TOTAL_LENGTH_LIMIT_UTF8 = METADATA_OBJECT_NAME_LENGTH_LIMIT_UTF8 * 4;
    public static final Pattern METADATA_OBJECT_NAME_INVALID_CHARS =
            Pattern.compile(SystemUtils.IS_OS_WINDOWS ? "[\u0000-\u001F\u007F\"*/:<>\\\\|+,;=\\[\\]\n]" : "[\u0000/]");

    // Pre-defined databases
    public static final String SYSTEM_DATABASE = "System";
    public static final String DEFAULT_DATABASE = "Default";

    // Name of the dataverse the metadata lives in.
    public static final DataverseName METADATA_DATAVERSE_NAME = DataverseName.createBuiltinDataverseName("Metadata");

    // Name of the pre-defined default dataverse
    public static final DataverseName DEFAULT_DATAVERSE_NAME = DataverseName.createBuiltinDataverseName("Default");

    public static final Namespace METADATA_NAMESPACE = new Namespace(SYSTEM_DATABASE, METADATA_DATAVERSE_NAME);
    public static final Namespace DEFAULT_NAMESPACE = new Namespace(DEFAULT_DATABASE, DEFAULT_DATAVERSE_NAME);

    // Name of the node group where metadata is stored on.
    public static final String METADATA_NODEGROUP_NAME = "MetadataGroup";

    public static final String DATABASE_DATASET_NAME = "Database";
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
    public static final String SYNONYM_DATASET_NAME = "Synonym";
    public static final String FULL_TEXT_CONFIG_DATASET_NAME = "FullTextConfig";
    public static final String FULL_TEXT_FILTER_DATASET_NAME = "FullTextFilter";

    public static final String PRIMARY_INDEX_PREFIX = "primary_idx_";
    public static final String SAMPLE_INDEX_PREFIX = "sample_idx_";
    public static final String SAMPLE_INDEX_1_PREFIX = SAMPLE_INDEX_PREFIX + "1_";
    public static final String SAMPLE_INDEX_2_PREFIX = SAMPLE_INDEX_PREFIX + "2_";

    private MetadataConstants() {
    }
}
