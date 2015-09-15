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
 * Contains static secondary-index descriptors on metadata datasets.
 */
public class MetadataSecondaryIndexes {
    public static IMetadataIndex GROUPNAME_ON_DATASET_INDEX;
    public static IMetadataIndex DATATYPENAME_ON_DATASET_INDEX;
    public static IMetadataIndex DATATYPENAME_ON_DATATYPE_INDEX;

    /**
     * Create all metadata secondary index descriptors. MetadataRecordTypes must
     * have been initialized before calling this init.
     * 
     * @throws MetadataException
     *             If MetadataRecordTypes have not been initialized.
     */
    public static void init() throws MetadataException {
        // Make sure the MetadataRecordTypes have been initialized.
        if (MetadataRecordTypes.DATASET_RECORDTYPE == null) {
            throw new MetadataException(
                    "Must initialize MetadataRecordTypes before initializing MetadataSecondaryIndexes.");
        }

        GROUPNAME_ON_DATASET_INDEX = new MetadataIndex("Dataset", "GroupName", 3, new IAType[] { BuiltinType.ASTRING,
                BuiltinType.ASTRING, BuiltinType.ASTRING }, (Arrays.asList(Arrays.asList("GroupName"),
                Arrays.asList("DataverseName"), Arrays.asList("DatasetName"))), 1, null,
                MetadataPrimaryIndexes.DATASET_DATASET_ID, false, new int[] { 1, 2 });

        DATATYPENAME_ON_DATASET_INDEX = new MetadataIndex("Dataset", "DatatypeName", 3, new IAType[] {
                BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING }, (Arrays.asList(
                Arrays.asList("DataverseName"), Arrays.asList("DatatypeName"), Arrays.asList("DatasetName"))), 2, null,
                MetadataPrimaryIndexes.DATASET_DATASET_ID, false, new int[] { 0, 2 });

        DATATYPENAME_ON_DATATYPE_INDEX = new MetadataIndex("Datatype", "DatatypeName", 3, new IAType[] {
                BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING },
                (Arrays.asList(Arrays.asList("DataverseName"), Arrays.asList("NestedDatatypeName"),
                        Arrays.asList("TopDatatypeName"))), 2, null, MetadataPrimaryIndexes.DATATYPE_DATASET_ID, false,
                new int[] { 0, 2 });

    }
}