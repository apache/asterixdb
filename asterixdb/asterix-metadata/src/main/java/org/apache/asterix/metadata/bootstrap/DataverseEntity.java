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

import static org.apache.asterix.metadata.bootstrap.MetadataPrimaryIndexes.PROPERTIES_DATAVERSE;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_DATABASE_NAME;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_DATAVERSE_NAME;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_DATA_FORMAT;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_PENDING_OP;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_TIMESTAMP;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.RECORD_NAME_DATAVERSE;

import java.util.List;

import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;

public final class DataverseEntity {

    private static final DataverseEntity DATAVERSE =
            new DataverseEntity(
                    new MetadataIndex(PROPERTIES_DATAVERSE, 2, new IAType[] { BuiltinType.ASTRING },
                            List.of(List.of(FIELD_NAME_DATAVERSE_NAME)), 0, dataverseType(), true, new int[] { 0 }),
                    1, -1);

    private static final DataverseEntity DB_DATAVERSE = new DataverseEntity(
            new MetadataIndex(PROPERTIES_DATAVERSE, 3, new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING },
                    List.of(List.of(FIELD_NAME_DATABASE_NAME), List.of(FIELD_NAME_DATAVERSE_NAME)), 0,
                    databaseDataverseType(), true, new int[] { 0, 1 }),
            2, 0);

    private final int payloadPosition;
    private final MetadataIndex index;
    private final int databaseNameIndex;
    private final int dataverseNameIndex;
    private final int dataFormatIndex;
    private final int timestampIndex;
    private final int pendingOpIndex;

    private DataverseEntity(MetadataIndex index, int payloadPosition, int startIndex) {
        this.index = index;
        this.payloadPosition = payloadPosition;
        this.databaseNameIndex = startIndex++;
        this.dataverseNameIndex = startIndex++;
        this.dataFormatIndex = startIndex++;
        this.timestampIndex = startIndex++;
        this.pendingOpIndex = startIndex++;
    }

    public static DataverseEntity of(boolean usingDatabase) {
        return usingDatabase ? DB_DATAVERSE : DATAVERSE;
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

    public int dataFormatIndex() {
        return dataFormatIndex;
    }

    public int timestampIndex() {
        return timestampIndex;
    }

    public int pendingOpIndex() {
        return pendingOpIndex;
    }

    private static ARecordType dataverseType() {
        return MetadataRecordTypes.createRecordType(
                // RecordTypeName
                RECORD_NAME_DATAVERSE,
                // FieldNames
                new String[] { FIELD_NAME_DATAVERSE_NAME, FIELD_NAME_DATA_FORMAT, FIELD_NAME_TIMESTAMP,
                        FIELD_NAME_PENDING_OP },
                // FieldTypes
                new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.AINT32 },
                //IsOpen?
                true);
    }

    private static ARecordType databaseDataverseType() {
        return MetadataRecordTypes.createRecordType(
                // RecordTypeName
                RECORD_NAME_DATAVERSE,
                // FieldNames
                new String[] { FIELD_NAME_DATABASE_NAME, FIELD_NAME_DATAVERSE_NAME, FIELD_NAME_DATA_FORMAT,
                        FIELD_NAME_TIMESTAMP, FIELD_NAME_PENDING_OP },
                // FieldTypes
                new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING,
                        BuiltinType.AINT32 },
                //IsOpen?
                true);
    }
}
