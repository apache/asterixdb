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

import static org.apache.asterix.metadata.bootstrap.MetadataPrimaryIndexes.PROPERTIES_DATASET;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.COMPACTION_POLICY_PROPERTIES_RECORDTYPE;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.DATASET_HINTS_RECORDTYPE;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.EXTERNAL_DETAILS_RECORDTYPE;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_COMPACTION_POLICY;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_COMPACTION_POLICY_PROPERTIES;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_DATABASE_NAME;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_DATASET_ID;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_DATASET_NAME;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_DATASET_TYPE;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_DATATYPE_DATAVERSE_NAME;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_DATATYPE_NAME;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_DATAVERSE_NAME;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_EXTERNAL_DETAILS;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_GROUP_NAME;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_HINTS;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_INTERNAL_DETAILS;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_PENDING_OP;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_TIMESTAMP;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.INTERNAL_DETAILS_RECORDTYPE;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.RECORD_NAME_DATASET;

import java.util.Arrays;
import java.util.List;

import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.AUnorderedListType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;

public final class DatasetEntity {

    private static final DatasetEntity DATASET = new DatasetEntity(
            new MetadataIndex(PROPERTIES_DATASET, 3, new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING },
                    Arrays.asList(List.of(FIELD_NAME_DATAVERSE_NAME), List.of(FIELD_NAME_DATASET_NAME)), 0,
                    datasetType(), true, new int[] { 0, 1 }),
            2, -1);

    private static final DatasetEntity DB_DATASET = new DatasetEntity(
            new MetadataIndex(PROPERTIES_DATASET, 4,
                    new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING },
                    Arrays.asList(List.of(FIELD_NAME_DATABASE_NAME), List.of(FIELD_NAME_DATAVERSE_NAME),
                            List.of(FIELD_NAME_DATASET_NAME)),
                    0, databaseDatasetType(), true, new int[] { 0, 1, 2 }),
            3, 0);

    private final int payloadPosition;
    private final MetadataIndex index;
    private final int databaseNameIndex;
    private final int dataverseNameIndex;
    private final int datasetNameIndex;
    private final int datatypeDataverseNameIndex;
    private final int datatypeNameIndex;
    private final int datasetTypeIndex;
    private final int groupNameIndex;
    private final int compactionPolicyIndex;
    private final int compactionPolicyPropertiesIndex;
    private final int internalDetailsIndex;
    private final int externalDetailsIndex;
    private final int hintsIndex;
    private final int timestampIndex;
    private final int datasetIdIndex;
    private final int pendingOpIndex;

    private DatasetEntity(MetadataIndex index, int payloadPosition, int startIndex) {
        this.index = index;
        this.payloadPosition = payloadPosition;
        this.databaseNameIndex = startIndex++;
        this.dataverseNameIndex = startIndex++;
        this.datasetNameIndex = startIndex++;
        this.datatypeDataverseNameIndex = startIndex++;
        this.datatypeNameIndex = startIndex++;
        this.datasetTypeIndex = startIndex++;
        this.groupNameIndex = startIndex++;
        this.compactionPolicyIndex = startIndex++;
        this.compactionPolicyPropertiesIndex = startIndex++;
        this.internalDetailsIndex = startIndex++;
        this.externalDetailsIndex = startIndex++;
        this.hintsIndex = startIndex++;
        this.timestampIndex = startIndex++;
        this.datasetIdIndex = startIndex++;
        this.pendingOpIndex = startIndex++;
    }

    public static DatasetEntity of(boolean cloudDeployment) {
        return DATASET;
    }

    public MetadataIndex getIndex() {
        return index;
    }

    public ARecordType getRecordType() {
        return index.getPayloadRecordType();
    }

    public int payloadPosition() {
        return payloadPosition;
    }

    public int databaseNameIndex() {
        return databaseNameIndex;
    }

    public int dataverseNameIndex() {
        return dataverseNameIndex;
    }

    public int datasetNameIndex() {
        return datasetNameIndex;
    }

    public int datatypeDataverseNameIndex() {
        return datatypeDataverseNameIndex;
    }

    public int datatypeNameIndex() {
        return datatypeNameIndex;
    }

    public int datasetTypeIndex() {
        return datasetTypeIndex;
    }

    public int groupNameIndex() {
        return groupNameIndex;
    }

    public int compactionPolicyIndex() {
        return compactionPolicyIndex;
    }

    public int compactionPolicyPropertiesIndex() {
        return compactionPolicyPropertiesIndex;
    }

    public int internalDetailsIndex() {
        return internalDetailsIndex;
    }

    public int externalDetailsIndex() {
        return externalDetailsIndex;
    }

    public int hintsIndex() {
        return hintsIndex;
    }

    public int timestampIndex() {
        return timestampIndex;
    }

    public int datasetIdIndex() {
        return datasetIdIndex;
    }

    public int pendingOpIndex() {
        return pendingOpIndex;
    }

    private static ARecordType datasetType() {
        return MetadataRecordTypes.createRecordType(
                // RecordTypeName
                RECORD_NAME_DATASET,
                // FieldNames
                new String[] { FIELD_NAME_DATAVERSE_NAME, FIELD_NAME_DATASET_NAME, FIELD_NAME_DATATYPE_DATAVERSE_NAME,
                        FIELD_NAME_DATATYPE_NAME, FIELD_NAME_DATASET_TYPE, FIELD_NAME_GROUP_NAME,
                        FIELD_NAME_COMPACTION_POLICY, FIELD_NAME_COMPACTION_POLICY_PROPERTIES,
                        FIELD_NAME_INTERNAL_DETAILS, FIELD_NAME_EXTERNAL_DETAILS, FIELD_NAME_HINTS,
                        FIELD_NAME_TIMESTAMP, FIELD_NAME_DATASET_ID, FIELD_NAME_PENDING_OP },
                // FieldTypes
                new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING,
                        BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING,
                        new AOrderedListType(COMPACTION_POLICY_PROPERTIES_RECORDTYPE, null),
                        AUnionType.createUnknownableType(INTERNAL_DETAILS_RECORDTYPE),
                        AUnionType.createUnknownableType(EXTERNAL_DETAILS_RECORDTYPE),
                        new AUnorderedListType(DATASET_HINTS_RECORDTYPE, null), BuiltinType.ASTRING, BuiltinType.AINT32,
                        BuiltinType.AINT32 },
                //IsOpen?
                true);
    }

    private static ARecordType databaseDatasetType() {
        return MetadataRecordTypes.createRecordType(
                // RecordTypeName
                RECORD_NAME_DATASET,
                // FieldNames
                new String[] { FIELD_NAME_DATABASE_NAME, FIELD_NAME_DATAVERSE_NAME, FIELD_NAME_DATASET_NAME,
                        FIELD_NAME_DATATYPE_DATAVERSE_NAME, FIELD_NAME_DATATYPE_NAME, FIELD_NAME_DATASET_TYPE,
                        FIELD_NAME_GROUP_NAME, FIELD_NAME_COMPACTION_POLICY, FIELD_NAME_COMPACTION_POLICY_PROPERTIES,
                        FIELD_NAME_INTERNAL_DETAILS, FIELD_NAME_EXTERNAL_DETAILS, FIELD_NAME_HINTS,
                        FIELD_NAME_TIMESTAMP, FIELD_NAME_DATASET_ID, FIELD_NAME_PENDING_OP },
                // FieldTypes
                new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING,
                        BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING,
                        new AOrderedListType(COMPACTION_POLICY_PROPERTIES_RECORDTYPE, null),
                        AUnionType.createUnknownableType(INTERNAL_DETAILS_RECORDTYPE),
                        AUnionType.createUnknownableType(EXTERNAL_DETAILS_RECORDTYPE),
                        new AUnorderedListType(DATASET_HINTS_RECORDTYPE, null), BuiltinType.ASTRING, BuiltinType.AINT32,
                        BuiltinType.AINT32 },
                //IsOpen?
                true);
    }
}
