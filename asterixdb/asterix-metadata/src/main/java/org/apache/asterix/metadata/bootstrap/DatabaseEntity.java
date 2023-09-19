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

import static org.apache.asterix.metadata.bootstrap.MetadataPrimaryIndexes.PROPERTIES_DATABASE;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_DATABASE_NAME;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_PENDING_OP;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_SYSTEM_DATABASE;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_TIMESTAMP;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.RECORD_NAME_DATABASE;

import java.util.List;

import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;

public final class DatabaseEntity {

    private static final DatabaseEntity DATABASE =
            new DatabaseEntity(
                    new MetadataIndex(PROPERTIES_DATABASE, 2, new IAType[] { BuiltinType.ASTRING },
                            List.of(List.of(FIELD_NAME_DATABASE_NAME)), 0, databaseType(), true, new int[] { 0 }),
                    1, 0);

    private final int payloadPosition;
    private final MetadataIndex index;
    private final int databaseNameIndex;
    private final int systemDatabaseIndex;
    private final int timestampIndex;
    private final int pendingOpIndex;

    private DatabaseEntity(MetadataIndex index, int payloadPosition, int startIndex) {
        this.index = index;
        this.payloadPosition = payloadPosition;
        this.databaseNameIndex = startIndex++;
        this.systemDatabaseIndex = startIndex++;
        this.timestampIndex = startIndex++;
        this.pendingOpIndex = startIndex++;
    }

    public static DatabaseEntity of(boolean cloudDeployment) {
        return DATABASE;
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

    public int systemDatabaseIndex() {
        return systemDatabaseIndex;
    }

    public int timestampIndex() {
        return timestampIndex;
    }

    public int pendingOpIndex() {
        return pendingOpIndex;
    }

    private static ARecordType databaseType() {
        return MetadataRecordTypes.createRecordType(
                // RecordTypeName
                RECORD_NAME_DATABASE,
                // FieldNames
                new String[] { FIELD_NAME_DATABASE_NAME, FIELD_NAME_SYSTEM_DATABASE, FIELD_NAME_TIMESTAMP,
                        FIELD_NAME_PENDING_OP },
                // FieldTypes
                new IAType[] { BuiltinType.ASTRING, BuiltinType.ABOOLEAN, BuiltinType.ASTRING, BuiltinType.AINT32 },
                //IsOpen?
                true);
    }
}
