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

import static org.apache.asterix.metadata.bootstrap.MetadataPrimaryIndexes.PROPERTIES_INDEX;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_DATABASE_NAME;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_DATASET_NAME;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_DATAVERSE_NAME;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_INDEX_NAME;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_INDEX_STRUCTURE;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_IS_PRIMARY;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_PENDING_OP;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_SEARCH_KEY;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_TIMESTAMP;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.RECORD_NAME_INDEX;

import java.util.Arrays;
import java.util.List;

import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;

public final class IndexEntity {

    private static final IndexEntity INDEX =
            new IndexEntity(
                    new MetadataIndex(PROPERTIES_INDEX, 4,
                            new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING },
                            Arrays.asList(List.of(FIELD_NAME_DATAVERSE_NAME), List.of(FIELD_NAME_DATASET_NAME),
                                    List.of(FIELD_NAME_INDEX_NAME)),
                            0, indexType(), true, new int[] { 0, 1, 2 }),
                    3, -1);

    private static final IndexEntity DB_INDEX = new IndexEntity(new MetadataIndex(PROPERTIES_INDEX, 5,
            new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING },
            Arrays.asList(List.of(FIELD_NAME_DATABASE_NAME), List.of(FIELD_NAME_DATAVERSE_NAME),
                    List.of(FIELD_NAME_DATASET_NAME), List.of(FIELD_NAME_INDEX_NAME)),
            0, databaseIndexType(), true, new int[] { 0, 1, 2, 3 }), 4, 0);

    private final int payloadPosition;
    private final MetadataIndex index;
    private final int databaseNameIndex;
    private final int dataverseNameIndex;
    private final int datasetNameIndex;
    private final int indexNameIndex;
    private final int indexStructureIndex;
    private final int searchKeyIndex;
    private final int isPrimaryIndex;
    private final int timestampIndex;
    private final int pendingOpIndex;

    private IndexEntity(MetadataIndex index, int payloadPosition, int startIndex) {
        this.index = index;
        this.payloadPosition = payloadPosition;
        this.databaseNameIndex = startIndex++;
        this.dataverseNameIndex = startIndex++;
        this.datasetNameIndex = startIndex++;
        this.indexNameIndex = startIndex++;
        this.indexStructureIndex = startIndex++;
        this.searchKeyIndex = startIndex++;
        this.isPrimaryIndex = startIndex++;
        this.timestampIndex = startIndex++;
        this.pendingOpIndex = startIndex++;
    }

    public static IndexEntity of(boolean cloudDeployment) {
        return INDEX;
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

    public int indexNameIndex() {
        return indexNameIndex;
    }

    public int indexStructureIndex() {
        return indexStructureIndex;
    }

    public int searchKeyIndex() {
        return searchKeyIndex;
    }

    public int isPrimaryIndex() {
        return isPrimaryIndex;
    }

    public int timestampIndex() {
        return timestampIndex;
    }

    public int pendingOpIndex() {
        return pendingOpIndex;
    }

    private static ARecordType indexType() {
        return MetadataRecordTypes.createRecordType(
                // RecordTypeName
                RECORD_NAME_INDEX,
                // FieldNames
                new String[] { FIELD_NAME_DATAVERSE_NAME, FIELD_NAME_DATASET_NAME, FIELD_NAME_INDEX_NAME,
                        FIELD_NAME_INDEX_STRUCTURE, FIELD_NAME_SEARCH_KEY, FIELD_NAME_IS_PRIMARY, FIELD_NAME_TIMESTAMP,
                        FIELD_NAME_PENDING_OP },
                // FieldTypes
                new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING,
                        new AOrderedListType(new AOrderedListType(BuiltinType.ASTRING, null), null),
                        BuiltinType.ABOOLEAN, BuiltinType.ASTRING, BuiltinType.AINT32 },
                //IsOpen?
                true);
    }

    private static ARecordType databaseIndexType() {
        return MetadataRecordTypes.createRecordType(
                // RecordTypeName
                RECORD_NAME_INDEX,
                // FieldNames
                new String[] { FIELD_NAME_DATABASE_NAME, FIELD_NAME_DATAVERSE_NAME, FIELD_NAME_DATASET_NAME,
                        FIELD_NAME_INDEX_NAME, FIELD_NAME_INDEX_STRUCTURE, FIELD_NAME_SEARCH_KEY, FIELD_NAME_IS_PRIMARY,
                        FIELD_NAME_TIMESTAMP, FIELD_NAME_PENDING_OP },
                // FieldTypes
                new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING,
                        BuiltinType.ASTRING,
                        new AOrderedListType(new AOrderedListType(BuiltinType.ASTRING, null), null),
                        BuiltinType.ABOOLEAN, BuiltinType.ASTRING, BuiltinType.AINT32 },
                //IsOpen?
                true);
    }
}
