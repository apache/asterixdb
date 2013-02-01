/*
 * Copyright 2009-2010 by The Regents of the University of California
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
                new String[] { "DataverseName" }, MetadataRecordTypes.DATAVERSE_RECORDTYPE);

        DATASET_DATASET = new MetadataIndex("Dataset", null, 3,
                new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING }, new String[] { "DataverseName",
                        "DatasetName" }, MetadataRecordTypes.DATASET_RECORDTYPE);

        DATATYPE_DATASET = new MetadataIndex("Datatype", null, 3, new IAType[] { BuiltinType.ASTRING,
                BuiltinType.ASTRING }, new String[] { "DataverseName", "DatatypeName" },
                MetadataRecordTypes.DATATYPE_RECORDTYPE);

        INDEX_DATASET = new MetadataIndex("Index", null, 4, new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING,
                BuiltinType.ASTRING }, new String[] { "DataverseName", "DatasetName", "IndexName" },
                MetadataRecordTypes.INDEX_RECORDTYPE);

        NODE_DATASET = new MetadataIndex("Node", null, 2, new IAType[] { BuiltinType.ASTRING },
                new String[] { "NodeName" }, MetadataRecordTypes.NODE_RECORDTYPE);

        NODEGROUP_DATASET = new MetadataIndex("Nodegroup", null, 2, new IAType[] { BuiltinType.ASTRING },
                new String[] { "GroupName" }, MetadataRecordTypes.NODEGROUP_RECORDTYPE);

        FUNCTION_DATASET = new MetadataIndex("Function", null, 4, new IAType[] { BuiltinType.ASTRING,
                BuiltinType.ASTRING, BuiltinType.ASTRING }, new String[] { "DataverseName", "Name", "Arity" },
                MetadataRecordTypes.FUNCTION_RECORDTYPE);

        DATASOURCE_ADAPTER_DATASET = new MetadataIndex("DatasourceAdapter", null, 3,
                new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING }, new String[] { "DataverseName", "Name" },
                MetadataRecordTypes.DATASOURCE_ADAPTER_RECORDTYPE);

    }
}