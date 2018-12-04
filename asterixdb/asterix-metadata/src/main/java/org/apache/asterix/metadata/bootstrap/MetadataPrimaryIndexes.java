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

import java.util.Arrays;

import org.apache.asterix.common.metadata.MetadataIndexImmutableProperties;
import org.apache.asterix.metadata.api.IMetadataIndex;
import org.apache.asterix.metadata.utils.MetadataConstants;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;

/**
 * Contains static primary-index descriptors of all metadata datasets.
 */
public class MetadataPrimaryIndexes {
    public static final MetadataIndexImmutableProperties PROPERTIES_METADATA =
            new MetadataIndexImmutableProperties(MetadataConstants.METADATA_DATAVERSE_NAME, 0, 0);
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

    public static final IMetadataIndex DATAVERSE_DATASET =
            new MetadataIndex(PROPERTIES_DATAVERSE, 2, new IAType[] { BuiltinType.ASTRING },
                    Arrays.asList(Arrays.asList(MetadataRecordTypes.FIELD_NAME_DATAVERSE_NAME)), 0,
                    MetadataRecordTypes.DATAVERSE_RECORDTYPE, true, new int[] { 0 });
    public static final IMetadataIndex DATASET_DATASET =
            new MetadataIndex(PROPERTIES_DATASET, 3, new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING },
                    Arrays.asList(Arrays.asList(MetadataRecordTypes.FIELD_NAME_DATAVERSE_NAME),
                            Arrays.asList(MetadataRecordTypes.FIELD_NAME_DATASET_NAME)),
                    0, MetadataRecordTypes.DATASET_RECORDTYPE, true, new int[] { 0, 1 });
    public static final IMetadataIndex DATATYPE_DATASET =
            new MetadataIndex(PROPERTIES_DATATYPE, 3, new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING },
                    Arrays.asList(Arrays.asList(MetadataRecordTypes.FIELD_NAME_DATAVERSE_NAME),
                            Arrays.asList(MetadataRecordTypes.FIELD_NAME_DATATYPE_NAME)),
                    0, MetadataRecordTypes.DATATYPE_RECORDTYPE, true, new int[] { 0, 1 });
    public static final IMetadataIndex INDEX_DATASET = new MetadataIndex(PROPERTIES_INDEX, 4,
            new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING },
            Arrays.asList(Arrays.asList(MetadataRecordTypes.FIELD_NAME_DATAVERSE_NAME),
                    Arrays.asList(MetadataRecordTypes.FIELD_NAME_DATASET_NAME),
                    Arrays.asList(MetadataRecordTypes.FIELD_NAME_INDEX_NAME)),
            0, MetadataRecordTypes.INDEX_RECORDTYPE, true, new int[] { 0, 1, 2 });
    public static final IMetadataIndex NODE_DATASET =
            new MetadataIndex(PROPERTIES_NODE, 2, new IAType[] { BuiltinType.ASTRING },
                    Arrays.asList(Arrays.asList(MetadataRecordTypes.FIELD_NAME_NODE_NAME)), 0,
                    MetadataRecordTypes.NODE_RECORDTYPE, true, new int[] { 0 });
    public static final IMetadataIndex NODEGROUP_DATASET =
            new MetadataIndex(PROPERTIES_NODEGROUP, 2, new IAType[] { BuiltinType.ASTRING },
                    Arrays.asList(Arrays.asList(MetadataRecordTypes.FIELD_NAME_GROUP_NAME)), 0,
                    MetadataRecordTypes.NODEGROUP_RECORDTYPE, true, new int[] { 0 });
    public static final IMetadataIndex FUNCTION_DATASET = new MetadataIndex(PROPERTIES_FUNCTION, 4,
            new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING },
            Arrays.asList(Arrays.asList(MetadataRecordTypes.FIELD_NAME_DATAVERSE_NAME),
                    Arrays.asList(MetadataRecordTypes.FIELD_NAME_NAME),
                    Arrays.asList(MetadataRecordTypes.FIELD_NAME_ARITY)),
            0, MetadataRecordTypes.FUNCTION_RECORDTYPE, true, new int[] { 0, 1, 2 });
    public static final IMetadataIndex DATASOURCE_ADAPTER_DATASET = new MetadataIndex(PROPERTIES_DATASOURCE_ADAPTER, 3,
            new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING },
            Arrays.asList(Arrays.asList(MetadataRecordTypes.FIELD_NAME_DATAVERSE_NAME),
                    Arrays.asList(MetadataRecordTypes.FIELD_NAME_NAME)),
            0, MetadataRecordTypes.DATASOURCE_ADAPTER_RECORDTYPE, true, new int[] { 0, 1 });
    public static final IMetadataIndex LIBRARY_DATASET =
            new MetadataIndex(PROPERTIES_LIBRARY, 3, new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING },
                    Arrays.asList(Arrays.asList(MetadataRecordTypes.FIELD_NAME_DATAVERSE_NAME),
                            Arrays.asList(MetadataRecordTypes.FIELD_NAME_NAME)),
                    0, MetadataRecordTypes.LIBRARY_RECORDTYPE, true, new int[] { 0, 1 });
    public static final IMetadataIndex FEED_DATASET =
            new MetadataIndex(PROPERTIES_FEED, 3, new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING },
                    Arrays.asList(Arrays.asList(MetadataRecordTypes.FIELD_NAME_DATAVERSE_NAME),
                            Arrays.asList(MetadataRecordTypes.FIELD_NAME_FEED_NAME)),
                    0, MetadataRecordTypes.FEED_RECORDTYPE, true, new int[] { 0, 1 });
    public static final IMetadataIndex FEED_ACTIVITY_DATASET = null;
    public static final IMetadataIndex FEED_POLICY_DATASET =
            new MetadataIndex(PROPERTIES_FEED_POLICY, 3, new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING },
                    Arrays.asList(Arrays.asList(MetadataRecordTypes.FIELD_NAME_DATAVERSE_NAME),
                            Arrays.asList(MetadataRecordTypes.FIELD_NAME_POLICY_NAME)),
                    0, MetadataRecordTypes.FEED_POLICY_RECORDTYPE, true, new int[] { 0, 1 });
    public static final IMetadataIndex COMPACTION_POLICY_DATASET = new MetadataIndex(PROPERTIES_COMPACTION_POLICY, 3,
            new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING },
            Arrays.asList(Arrays.asList(MetadataRecordTypes.FIELD_NAME_DATAVERSE_NAME),
                    Arrays.asList(MetadataRecordTypes.FIELD_NAME_COMPACTION_POLICY)),
            0, MetadataRecordTypes.COMPACTION_POLICY_RECORDTYPE, true, new int[] { 0, 1 });
    public static final IMetadataIndex EXTERNAL_FILE_DATASET = new MetadataIndex(PROPERTIES_EXTERNAL_FILE, 4,
            new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.AINT32 },
            Arrays.asList(Arrays.asList(MetadataRecordTypes.FIELD_NAME_DATAVERSE_NAME),
                    Arrays.asList(MetadataRecordTypes.FIELD_NAME_DATASET_NAME),
                    Arrays.asList(MetadataRecordTypes.FIELD_NAME_FILE_NUMBER)),
            0, MetadataRecordTypes.EXTERNAL_FILE_RECORDTYPE, true, new int[] { 0, 1, 2 });

    public static final IMetadataIndex FEED_CONNECTION_DATASET = new MetadataIndex(PROPERTIES_FEED_CONNECTION, 4,
            new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING },
            Arrays.asList(Arrays.asList(MetadataRecordTypes.FIELD_NAME_DATAVERSE_NAME),
                    Arrays.asList(MetadataRecordTypes.FIELD_NAME_FEED_NAME),
                    Arrays.asList(MetadataRecordTypes.FIELD_NAME_DATASET_NAME)),
            0, MetadataRecordTypes.FEED_CONNECTION_RECORDTYPE, true, new int[] { 0, 1, 2 });

    private MetadataPrimaryIndexes() {
    }
}
