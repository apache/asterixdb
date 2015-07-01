/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.asterix.metadata.bootstrap;

import java.util.Arrays;

import edu.uci.ics.asterix.metadata.MetadataException;
import edu.uci.ics.asterix.metadata.api.IMetadataIndex;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;

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

    public static final int METADATA_DATASET_ID = 0;
    public static final int DATAVERSE_DATASET_ID = 1;
    public static final int DATASET_DATASET_ID = 2;
    public static final int DATATYPE_DATASET_ID = 3;
    public static final int INDEX_DATASET_ID = 4;
    public static final int NODE_DATASET_ID = 5;
    public static final int NODEGROUP_DATASET_ID = 6;
    public static final int FUNCTION_DATASET_ID = 7;
    public static final int DATASOURCE_ADAPTER_DATASET_ID = 8;

    public static final int LIBRARY_DATASET_ID = 9;
    public static final int FEED_DATASET_ID = 10;
    public static final int FEED_ACTIVITY_DATASET_ID = 11;
    public static final int FEED_POLICY_DATASET_ID = 12;
    public static final int COMPACTION_POLICY_DATASET_ID = 13;
    public static final int EXTERNAL_FILE_DATASET_ID = 14;

    public static final int FIRST_AVAILABLE_USER_DATASET_ID = 100;

    /**
     * Create all metadata primary index descriptors. MetadataRecordTypes must
     * have been initialized before calling this init.
     * 
     * @throws MetadataException
     *             If MetadataRecordTypes have not been initialized.
     */
    public static void init() throws MetadataException {
        // Make sure the MetadataRecordTypes have been initialized.
        if (MetadataRecordTypes.DATASET_RECORDTYPE == null) {
            throw new MetadataException(
                    "Must initialize MetadataRecordTypes before initializing MetadataPrimaryIndexes");
        }

        DATAVERSE_DATASET = new MetadataIndex("Dataverse", null, 2, new IAType[] { BuiltinType.ASTRING },
                (Arrays.asList(Arrays.asList("DataverseName"))), 0, MetadataRecordTypes.DATAVERSE_RECORDTYPE, DATAVERSE_DATASET_ID,
                true, new int[] { 0 });

        DATASET_DATASET = new MetadataIndex("Dataset", null, 3,
                new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING }, (Arrays.asList(Arrays.asList("DataverseName"),Arrays.asList(
                        "DatasetName"))), 0, MetadataRecordTypes.DATASET_RECORDTYPE, DATASET_DATASET_ID, true,
                new int[] { 0, 1 });

        DATATYPE_DATASET = new MetadataIndex("Datatype", null, 3, new IAType[] { BuiltinType.ASTRING,
                BuiltinType.ASTRING }, (Arrays.asList(Arrays.asList("DataverseName"),Arrays.asList("DatatypeName" ))), 0,
                MetadataRecordTypes.DATATYPE_RECORDTYPE, DATATYPE_DATASET_ID, true, new int[] { 0, 1 });

        INDEX_DATASET = new MetadataIndex("Index", null, 4, new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING,
                BuiltinType.ASTRING }, (Arrays.asList(Arrays.asList("DataverseName"),Arrays.asList("DatasetName"),Arrays.asList("IndexName"))), 0,
                MetadataRecordTypes.INDEX_RECORDTYPE, INDEX_DATASET_ID, true, new int[] { 0, 1, 2 });

        NODE_DATASET = new MetadataIndex("Node", null, 2, new IAType[] { BuiltinType.ASTRING },
                (Arrays.asList(Arrays.asList("NodeName"))), 0, MetadataRecordTypes.NODE_RECORDTYPE, NODE_DATASET_ID, true,
                new int[] { 0 });

        NODEGROUP_DATASET = new MetadataIndex("Nodegroup", null, 2, new IAType[] { BuiltinType.ASTRING },
                (Arrays.asList(Arrays.asList("GroupName"))), 0, MetadataRecordTypes.NODEGROUP_RECORDTYPE, NODEGROUP_DATASET_ID, true,
                new int[] { 0 });

        FUNCTION_DATASET = new MetadataIndex("Function", null, 4, new IAType[] { BuiltinType.ASTRING,
                BuiltinType.ASTRING, BuiltinType.ASTRING }, (Arrays.asList(Arrays.asList("DataverseName"),Arrays.asList("Name"),Arrays.asList("Arity"))), 0,
                MetadataRecordTypes.FUNCTION_RECORDTYPE, FUNCTION_DATASET_ID, true, new int[] { 0, 1, 2 });

        DATASOURCE_ADAPTER_DATASET = new MetadataIndex("DatasourceAdapter", null, 3, new IAType[] {
                BuiltinType.ASTRING, BuiltinType.ASTRING }, (Arrays.asList(Arrays.asList("DataverseName"),Arrays.asList("Name"))), 0,
                MetadataRecordTypes.DATASOURCE_ADAPTER_RECORDTYPE, DATASOURCE_ADAPTER_DATASET_ID, true, new int[] { 0,
                        1 });

        FEED_DATASET = new MetadataIndex("Feed", null, 3, new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING },
                (Arrays.asList(Arrays.asList("DataverseName"),Arrays.asList("FeedName"))), 0, MetadataRecordTypes.FEED_RECORDTYPE, FEED_DATASET_ID,
                true, new int[] { 0, 1 });

        FEED_ACTIVITY_DATASET = new MetadataIndex("FeedActivity", null, 5, new IAType[] { BuiltinType.ASTRING,
                BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.AINT32 }, (Arrays.asList(Arrays.asList("DataverseName"),Arrays.asList(
                "FeedName"),Arrays.asList("DatasetName"),Arrays.asList("ActivityId"))), 0, MetadataRecordTypes.FEED_ACTIVITY_RECORDTYPE,
                FEED_ACTIVITY_DATASET_ID, true, new int[] { 0, 1, 2, 3 });

        LIBRARY_DATASET = new MetadataIndex("Library", null, 3,
                new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING }, (Arrays.asList(Arrays.asList("DataverseName"),Arrays.asList("Name"))), 0,
                MetadataRecordTypes.LIBRARY_RECORDTYPE, LIBRARY_DATASET_ID, true, new int[] { 0, 1 });

        FEED_POLICY_DATASET = new MetadataIndex("FeedPolicy", null, 3, new IAType[] { BuiltinType.ASTRING,
                BuiltinType.ASTRING }, (Arrays.asList(Arrays.asList("DataverseName"),Arrays.asList("PolicyName"))), 0,
                MetadataRecordTypes.FEED_POLICY_RECORDTYPE, FEED_POLICY_DATASET_ID, true, new int[] { 0, 1 });

        COMPACTION_POLICY_DATASET = new MetadataIndex("CompactionPolicy", null, 3, new IAType[] { BuiltinType.ASTRING,
                BuiltinType.ASTRING }, (Arrays.asList(Arrays.asList("DataverseName"),Arrays.asList("CompactionPolicy"))), 0,
                MetadataRecordTypes.COMPACTION_POLICY_RECORDTYPE, COMPACTION_POLICY_DATASET_ID, true,
                new int[] { 0, 1 });
        
        EXTERNAL_FILE_DATASET = new MetadataIndex("ExternalFile", null, 4, new IAType[] { BuiltinType.ASTRING,
                BuiltinType.ASTRING, BuiltinType.AINT32 },
                (Arrays.asList(Arrays.asList("DataverseName"),Arrays.asList("DatasetName"),Arrays.asList("FileNumber"))), 0,
                MetadataRecordTypes.EXTERNAL_FILE_RECORDTYPE, EXTERNAL_FILE_DATASET_ID, true, new int[] { 0, 1, 2 });
    }
}