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

import org.apache.asterix.metadata.MetadataException;
import org.apache.asterix.metadata.api.IMetadataIndex;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;

/**
 * Contains static primary-index descriptors of all metadata datasets.
 */
public class MetadataPrimaryIndexes {

    public static IMetadataIndex DATAVERSE_DATASET;
    public static IMetadataIndex DATASET_DATASET;
    public static IMetadataIndex DATATYPE_DATASET;
    public static IMetadataIndex INDEX_DATASET;
    public static IMetadataIndex NODE_DATASET;
    public static IMetadataIndex NODEGROUP_DATASET;
    public static IMetadataIndex FUNCTION_DATASET;
    public static IMetadataIndex DATASOURCE_ADAPTER_DATASET;
    public static IMetadataIndex LIBRARY_DATASET;
    public static IMetadataIndex FEED_DATASET;
    public static IMetadataIndex FEED_ACTIVITY_DATASET;
    public static IMetadataIndex FEED_POLICY_DATASET;
    public static IMetadataIndex COMPACTION_POLICY_DATASET;
    public static IMetadataIndex EXTERNAL_FILE_DATASET;

    /**
     * Create all metadata primary index descriptors. MetadataRecordTypes must
     * have been initialized before calling this init.
     * @throws MetadataException
     *             If MetadataRecordTypes have not been initialized.
     */
    public static void init() throws MetadataException {
        // Make sure the MetadataRecordTypes have been initialized.
        if (MetadataRecordTypes.DATASET_RECORDTYPE == null) {
            throw new MetadataException(
                    "Must initialize MetadataRecordTypes before initializing MetadataPrimaryIndexes");
        }

        DATAVERSE_DATASET = new MetadataIndex(MetadataIndexImmutableProperties.DATAVERSE, 2,
                new IAType[] { BuiltinType.ASTRING }, (Arrays.asList(Arrays.asList("DataverseName"))), 0,
                MetadataRecordTypes.DATAVERSE_RECORDTYPE, true, new int[] { 0 });

        DATASET_DATASET = new MetadataIndex(MetadataIndexImmutableProperties.DATASET, 3,
                new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING },
                (Arrays.asList(Arrays.asList("DataverseName"), Arrays.asList("DatasetName"))), 0,
                MetadataRecordTypes.DATASET_RECORDTYPE, true, new int[] { 0, 1 });

        DATATYPE_DATASET = new MetadataIndex(MetadataIndexImmutableProperties.DATATYPE, 3,
                new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING },
                (Arrays.asList(Arrays.asList("DataverseName"), Arrays.asList("DatatypeName"))), 0,
                MetadataRecordTypes.DATATYPE_RECORDTYPE, true, new int[] { 0, 1 });

        INDEX_DATASET = new MetadataIndex(MetadataIndexImmutableProperties.INDEX, 4,
                new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING },
                (Arrays.asList(Arrays.asList("DataverseName"), Arrays.asList("DatasetName"),
                        Arrays.asList("IndexName"))),
                0, MetadataRecordTypes.INDEX_RECORDTYPE, true, new int[] { 0, 1, 2 });

        NODE_DATASET = new MetadataIndex(MetadataIndexImmutableProperties.NODE, 2, new IAType[] { BuiltinType.ASTRING },
                (Arrays.asList(Arrays.asList("NodeName"))), 0, MetadataRecordTypes.NODE_RECORDTYPE, true,
                new int[] { 0 });

        NODEGROUP_DATASET = new MetadataIndex(MetadataIndexImmutableProperties.NODEGROUP, 2,
                new IAType[] { BuiltinType.ASTRING }, (Arrays.asList(Arrays.asList("GroupName"))), 0,
                MetadataRecordTypes.NODEGROUP_RECORDTYPE, true, new int[] { 0 });

        FUNCTION_DATASET = new MetadataIndex(MetadataIndexImmutableProperties.FUNCTION, 4,
                new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING },
                (Arrays.asList(Arrays.asList("DataverseName"), Arrays.asList("Name"), Arrays.asList("Arity"))), 0,
                MetadataRecordTypes.FUNCTION_RECORDTYPE, true, new int[] { 0, 1, 2 });

        DATASOURCE_ADAPTER_DATASET = new MetadataIndex(MetadataIndexImmutableProperties.DATASOURCE_ADAPTER, 3,
                new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING },
                (Arrays.asList(Arrays.asList("DataverseName"), Arrays.asList("Name"))), 0,
                MetadataRecordTypes.DATASOURCE_ADAPTER_RECORDTYPE, true,
                new int[] { 0, 1 });

        FEED_DATASET = new MetadataIndex(MetadataIndexImmutableProperties.FEED, 3,
                new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING },
                (Arrays.asList(Arrays.asList("DataverseName"), Arrays.asList("FeedName"))), 0,
                MetadataRecordTypes.FEED_RECORDTYPE, true, new int[] { 0, 1 });

        LIBRARY_DATASET = new MetadataIndex(MetadataIndexImmutableProperties.LIBRARY, 3,
                new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING },
                (Arrays.asList(Arrays.asList("DataverseName"), Arrays.asList("Name"))), 0,
                MetadataRecordTypes.LIBRARY_RECORDTYPE, true, new int[] { 0, 1 });

        FEED_POLICY_DATASET = new MetadataIndex(MetadataIndexImmutableProperties.FEED_POLICY, 3,
                new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING },
                (Arrays.asList(Arrays.asList("DataverseName"), Arrays.asList("PolicyName"))), 0,
                MetadataRecordTypes.FEED_POLICY_RECORDTYPE, true, new int[] { 0, 1 });

        COMPACTION_POLICY_DATASET = new MetadataIndex(MetadataIndexImmutableProperties.COMPACTION_POLICY, 3,
                new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING },
                (Arrays.asList(Arrays.asList("DataverseName"), Arrays.asList("CompactionPolicy"))), 0,
                MetadataRecordTypes.COMPACTION_POLICY_RECORDTYPE, true,
                new int[] { 0, 1 });

        EXTERNAL_FILE_DATASET = new MetadataIndex(MetadataIndexImmutableProperties.EXTERNAL_FILE, 4,
                new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.AINT32 },
                (Arrays.asList(Arrays.asList("DataverseName"), Arrays.asList("DatasetName"),
                        Arrays.asList("FileNumber"))),
                0, MetadataRecordTypes.EXTERNAL_FILE_RECORDTYPE, true, new int[] { 0, 1, 2 });
    }
}