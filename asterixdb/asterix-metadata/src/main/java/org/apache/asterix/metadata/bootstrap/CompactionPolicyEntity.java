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

import static org.apache.asterix.metadata.bootstrap.MetadataPrimaryIndexes.PROPERTIES_COMPACTION_POLICY;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_CLASSNAME;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_COMPACTION_POLICY;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_DATABASE_NAME;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_DATAVERSE_NAME;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.RECORD_NAME_COMPACTION_POLICY;

import java.util.Arrays;
import java.util.List;

import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;

public final class CompactionPolicyEntity {

    private static final CompactionPolicyEntity COMPACTION_POLICY =
            new CompactionPolicyEntity(new MetadataIndex(PROPERTIES_COMPACTION_POLICY, 3,
                    new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING },
                    Arrays.asList(List.of(FIELD_NAME_DATAVERSE_NAME), List.of(FIELD_NAME_COMPACTION_POLICY)), 0,
                    compactionPolicyType(), true, new int[] { 0, 1 }), 2, -1);

    private static final CompactionPolicyEntity DB_COMPACTION_POLICY =
            new CompactionPolicyEntity(new MetadataIndex(PROPERTIES_COMPACTION_POLICY, 4,
                    new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING },
                    Arrays.asList(List.of(FIELD_NAME_DATABASE_NAME), List.of(FIELD_NAME_DATAVERSE_NAME),
                            List.of(FIELD_NAME_COMPACTION_POLICY)),
                    0, databaseCompactionPolicyType(), true, new int[] { 0, 1, 2 }), 3, 0);

    private final int payloadPosition;
    private final MetadataIndex index;
    private final int databaseNameIndex;
    private final int dataverseNameIndex;
    private final int policyNameIndex;
    private final int classNameIndex;

    private CompactionPolicyEntity(MetadataIndex index, int payloadPosition, int startIndex) {
        this.index = index;
        this.payloadPosition = payloadPosition;
        this.databaseNameIndex = startIndex++;
        this.dataverseNameIndex = startIndex++;
        this.policyNameIndex = startIndex++;
        this.classNameIndex = startIndex++;
    }

    public static CompactionPolicyEntity of(boolean usingDatabase) {
        return usingDatabase ? DB_COMPACTION_POLICY : COMPACTION_POLICY;
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

    public int policyNameIndex() {
        return policyNameIndex;
    }

    public int classNameIndex() {
        return classNameIndex;
    }

    private static ARecordType compactionPolicyType() {
        return MetadataRecordTypes.createRecordType(
                // RecordTypeName
                RECORD_NAME_COMPACTION_POLICY,
                // FieldNames
                new String[] { FIELD_NAME_DATAVERSE_NAME, FIELD_NAME_COMPACTION_POLICY, FIELD_NAME_CLASSNAME },
                // FieldTypes
                new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING },
                //IsOpen?
                true);
    }

    private static ARecordType databaseCompactionPolicyType() {
        return MetadataRecordTypes.createRecordType(
                // RecordTypeName
                RECORD_NAME_COMPACTION_POLICY,
                // FieldNames
                new String[] { FIELD_NAME_DATABASE_NAME, FIELD_NAME_DATAVERSE_NAME, FIELD_NAME_COMPACTION_POLICY,
                        FIELD_NAME_CLASSNAME },
                // FieldTypes
                new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING },
                //IsOpen?
                true);
    }
}
