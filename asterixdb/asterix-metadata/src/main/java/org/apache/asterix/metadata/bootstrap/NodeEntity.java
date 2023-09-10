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

import static org.apache.asterix.metadata.bootstrap.MetadataPrimaryIndexes.PROPERTIES_NODE;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_NODE_NAME;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_NUMBER_OF_CORES;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_WORKING_MEMORY_SIZE;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.RECORD_NAME_NODE;

import java.util.List;

import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;

public final class NodeEntity {

    private static final NodeEntity NODE =
            new NodeEntity(new MetadataIndex(PROPERTIES_NODE, 2, new IAType[] { BuiltinType.ASTRING },
                    List.of(List.of(FIELD_NAME_NODE_NAME)), 0, nodeType(), true, new int[] { 0 }), 1, -1);

    private final int payloadPosition;
    private final MetadataIndex index;
    private final int databaseNameIndex;
    private final int nodeNameIndex;
    private final int numberOfCoresIndex;
    private final int memorySizeIndex;

    private NodeEntity(MetadataIndex index, int payloadPosition, int startIndex) {
        this.index = index;
        this.payloadPosition = payloadPosition;
        this.databaseNameIndex = startIndex++;
        this.nodeNameIndex = startIndex++;
        this.numberOfCoresIndex = startIndex++;
        this.memorySizeIndex = startIndex++;
    }

    public static NodeEntity of(boolean cloudDeployment) {
        return NODE;
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

    public int nodeNameIndex() {
        return nodeNameIndex;
    }

    public int numberOfCoresIndex() {
        return numberOfCoresIndex;
    }

    public int memorySizeIndex() {
        return memorySizeIndex;
    }

    private static ARecordType nodeType() {
        return MetadataRecordTypes.createRecordType(
                // RecordTypeName
                RECORD_NAME_NODE,
                // FieldNames
                new String[] { FIELD_NAME_NODE_NAME, FIELD_NAME_NUMBER_OF_CORES, FIELD_NAME_WORKING_MEMORY_SIZE },
                // FieldTypes
                new IAType[] { BuiltinType.ASTRING, BuiltinType.AINT64, BuiltinType.AINT64 },
                //IsOpen?
                true);
    }
}
