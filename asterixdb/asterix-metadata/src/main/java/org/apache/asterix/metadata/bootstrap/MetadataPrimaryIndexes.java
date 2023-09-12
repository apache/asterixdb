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

import org.apache.asterix.common.metadata.MetadataIndexImmutableProperties;
import org.apache.asterix.metadata.utils.MetadataConstants;

/**
 * Contains static primary-index descriptors of all metadata datasets.
 */
public class MetadataPrimaryIndexes {
    public static final MetadataIndexImmutableProperties PROPERTIES_DATAVERSE =
            new MetadataIndexImmutableProperties(MetadataConstants.DATAVERSE_DATASET_NAME, 1, 1);
    public static final MetadataIndexImmutableProperties PROPERTIES_DATASET =
            new MetadataIndexImmutableProperties(MetadataConstants.DATASET_DATASET_NAME, 2, 2);
    public static final MetadataIndexImmutableProperties PROPERTIES_DATATYPE =
            new MetadataIndexImmutableProperties(MetadataConstants.DATATYPE_DATASET_NAME, 3, 3);
    public static final MetadataIndexImmutableProperties PROPERTIES_INDEX =
            new MetadataIndexImmutableProperties(MetadataConstants.INDEX_DATASET_NAME, 4, 4);
    public static final MetadataIndexImmutableProperties PROPERTIES_NODE =
            new MetadataIndexImmutableProperties(MetadataConstants.NODE_DATASET_NAME, 5, 5);
    public static final MetadataIndexImmutableProperties PROPERTIES_NODEGROUP =
            new MetadataIndexImmutableProperties(MetadataConstants.NODEGROUP_DATASET_NAME, 6, 6);
    public static final MetadataIndexImmutableProperties PROPERTIES_FUNCTION =
            new MetadataIndexImmutableProperties(MetadataConstants.FUNCTION_DATASET_NAME, 7, 7);
    public static final MetadataIndexImmutableProperties PROPERTIES_DATASOURCE_ADAPTER =
            new MetadataIndexImmutableProperties(MetadataConstants.DATASOURCE_ADAPTER_DATASET_NAME, 8, 8);
    public static final MetadataIndexImmutableProperties PROPERTIES_LIBRARY =
            new MetadataIndexImmutableProperties(MetadataConstants.LIBRARY_DATASET_NAME, 9, 9);
    public static final MetadataIndexImmutableProperties PROPERTIES_FEED =
            new MetadataIndexImmutableProperties(MetadataConstants.FEED_DATASET_NAME, 10, 10);
    public static final MetadataIndexImmutableProperties PROPERTIES_FEED_CONNECTION =
            new MetadataIndexImmutableProperties(MetadataConstants.FEED_CONNECTION_DATASET_NAME, 11, 11);
    public static final MetadataIndexImmutableProperties PROPERTIES_FEED_POLICY =
            new MetadataIndexImmutableProperties(MetadataConstants.FEED_POLICY_DATASET_NAME, 12, 12);
    public static final MetadataIndexImmutableProperties PROPERTIES_COMPACTION_POLICY =
            new MetadataIndexImmutableProperties(MetadataConstants.COMPACTION_POLICY_DATASET_NAME, 13, 13);
    public static final MetadataIndexImmutableProperties PROPERTIES_EXTERNAL_FILE =
            new MetadataIndexImmutableProperties(MetadataConstants.EXTERNAL_FILE_DATASET_NAME, 14, 14);
    public static final MetadataIndexImmutableProperties PROPERTIES_SYNONYM =
            new MetadataIndexImmutableProperties(MetadataConstants.SYNONYM_DATASET_NAME, 15, 15);
    public static final MetadataIndexImmutableProperties PROPERTIES_FULL_TEXT_CONFIG =
            new MetadataIndexImmutableProperties(MetadataConstants.FULL_TEXT_CONFIG_DATASET_NAME, 16, 16);
    public static final MetadataIndexImmutableProperties PROPERTIES_FULL_TEXT_FILTER =
            new MetadataIndexImmutableProperties(MetadataConstants.FULL_TEXT_FILTER_DATASET_NAME, 17, 17);
    public static final MetadataIndexImmutableProperties PROPERTIES_DATABASE =
            new MetadataIndexImmutableProperties(MetadataConstants.DATABASE_DATASET_NAME, 18, 18);

    private MetadataPrimaryIndexes() {
    }
}
