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

import static org.apache.asterix.metadata.bootstrap.MetadataPrimaryIndexes.PROPERTIES_NODEGROUP;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_GROUP_NAME;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_NODE_NAMES;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_TIMESTAMP;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.RECORD_NAME_NODE_GROUP;

import java.util.List;

import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.AUnorderedListType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;

public final class NodeGroupEntity {

    private static final NodeGroupEntity NODE_GROUP =
            new NodeGroupEntity(new MetadataIndex(PROPERTIES_NODEGROUP, 2, new IAType[] { BuiltinType.ASTRING },
                    List.of(List.of(FIELD_NAME_GROUP_NAME)), 0, nodeGroupType(), true, new int[] { 0 }), 1, -1);

    private final int payloadPosition;
    private final MetadataIndex index;
    private final int databaseNameIndex;
    private final int groupNameIndex;
    private final int nodeNamesIndex;
    private final int timestampIndex;

    private NodeGroupEntity(MetadataIndex index, int payloadPosition, int startIndex) {
        this.index = index;
        this.payloadPosition = payloadPosition;
        this.databaseNameIndex = startIndex++;
        this.groupNameIndex = startIndex++;
        this.nodeNamesIndex = startIndex++;
        this.timestampIndex = startIndex++;
    }

    public static NodeGroupEntity of(boolean cloudDeployment) {
        return NODE_GROUP;
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

    public int groupNameIndex() {
        return groupNameIndex;
    }

    public int nodeNamesIndex() {
        return nodeNamesIndex;
    }

    public int timestampIndex() {
        return timestampIndex;
    }

    private static ARecordType nodeGroupType() {
        return MetadataRecordTypes.createRecordType(
                // RecordTypeName
                RECORD_NAME_NODE_GROUP,
                // FieldNames
                new String[] { FIELD_NAME_GROUP_NAME, FIELD_NAME_NODE_NAMES, FIELD_NAME_TIMESTAMP },
                // FieldTypes
                new IAType[] { BuiltinType.ASTRING, new AUnorderedListType(BuiltinType.ASTRING, null),
                        BuiltinType.ASTRING },
                //IsOpen?
                true);
    }
}
