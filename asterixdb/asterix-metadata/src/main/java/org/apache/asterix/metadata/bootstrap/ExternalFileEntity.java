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

import static org.apache.asterix.metadata.bootstrap.MetadataPrimaryIndexes.PROPERTIES_EXTERNAL_FILE;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_DATABASE_NAME;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_DATASET_NAME;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_DATAVERSE_NAME;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_FILE_MOD_TIME;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_FILE_NAME;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_FILE_NUMBER;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_FILE_SIZE;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_PENDING_OP;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.RECORD_NAME_EXTERNAL_FILE;

import java.util.Arrays;
import java.util.List;

import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;

public final class ExternalFileEntity {

    private static final ExternalFileEntity EXTERNAL_FILE = new ExternalFileEntity(
            new MetadataIndex(PROPERTIES_EXTERNAL_FILE, 4,
                    new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.AINT32 },
                    Arrays.asList(List.of(FIELD_NAME_DATAVERSE_NAME), List.of(FIELD_NAME_DATASET_NAME),
                            List.of(FIELD_NAME_FILE_NUMBER)),
                    0, externalFileType(), true, new int[] { 0, 1, 2 }),
            3, -1);

    private static final ExternalFileEntity DB_EXTERNAL_FILE =
            new ExternalFileEntity(new MetadataIndex(PROPERTIES_EXTERNAL_FILE, 5,
                    new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.AINT32 },
                    Arrays.asList(List.of(FIELD_NAME_DATABASE_NAME), List.of(FIELD_NAME_DATAVERSE_NAME),
                            List.of(FIELD_NAME_DATASET_NAME), List.of(FIELD_NAME_FILE_NUMBER)),
                    0, databaseExternalFileType(), true, new int[] { 0, 1, 2, 3 }), 4, 0);

    private final int payloadPosition;
    private final MetadataIndex index;
    private final int databaseNameIndex;
    private final int dataverseNameIndex;
    private final int datasetNameIndex;
    private final int fileNumberIndex;
    private final int fileNameIndex;
    private final int fileSizeIndex;
    private final int fileModDateIndex;
    private final int pendingOpIndex;

    private ExternalFileEntity(MetadataIndex index, int payloadPosition, int startIndex) {
        this.index = index;
        this.payloadPosition = payloadPosition;
        this.databaseNameIndex = startIndex++;
        this.dataverseNameIndex = startIndex++;
        this.datasetNameIndex = startIndex++;
        this.fileNumberIndex = startIndex++;
        this.fileNameIndex = startIndex++;
        this.fileSizeIndex = startIndex++;
        this.fileModDateIndex = startIndex++;
        this.pendingOpIndex = startIndex++;
    }

    public static ExternalFileEntity of(boolean usingDatabase) {
        return usingDatabase ? DB_EXTERNAL_FILE : EXTERNAL_FILE;
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

    public int fileNumberIndex() {
        return fileNumberIndex;
    }

    public int fileNameIndex() {
        return fileNameIndex;
    }

    public int fileSizeIndex() {
        return fileSizeIndex;
    }

    public int fileModDateIndex() {
        return fileModDateIndex;
    }

    public int pendingOpIndex() {
        return pendingOpIndex;
    }

    private static ARecordType externalFileType() {
        return MetadataRecordTypes.createRecordType(
                // RecordTypeName
                RECORD_NAME_EXTERNAL_FILE,
                // FieldNames
                new String[] { FIELD_NAME_DATAVERSE_NAME, FIELD_NAME_DATASET_NAME, FIELD_NAME_FILE_NUMBER,
                        FIELD_NAME_FILE_NAME, FIELD_NAME_FILE_SIZE, FIELD_NAME_FILE_MOD_TIME, FIELD_NAME_PENDING_OP },
                // FieldTypes
                new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.AINT32, BuiltinType.ASTRING,
                        BuiltinType.AINT64, BuiltinType.ADATETIME, BuiltinType.AINT32 },
                //IsOpen?
                true);
    }

    private static ARecordType databaseExternalFileType() {
        return MetadataRecordTypes.createRecordType(
                // RecordTypeName
                RECORD_NAME_EXTERNAL_FILE,
                // FieldNames
                new String[] { FIELD_NAME_DATABASE_NAME, FIELD_NAME_DATAVERSE_NAME, FIELD_NAME_DATASET_NAME,
                        FIELD_NAME_FILE_NUMBER, FIELD_NAME_FILE_NAME, FIELD_NAME_FILE_SIZE, FIELD_NAME_FILE_MOD_TIME,
                        FIELD_NAME_PENDING_OP },
                // FieldTypes
                new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.AINT32,
                        BuiltinType.ASTRING, BuiltinType.AINT64, BuiltinType.ADATETIME, BuiltinType.AINT32 },
                //IsOpen?
                true);
    }
}
