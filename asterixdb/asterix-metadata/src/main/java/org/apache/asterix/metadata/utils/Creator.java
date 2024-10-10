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
package org.apache.asterix.metadata.utils;

import java.io.Serializable;

import org.apache.asterix.common.metadata.MetadataConstants;
import org.apache.asterix.metadata.bootstrap.MetadataRecordTypes;
import org.apache.asterix.om.base.ARecord;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.types.ARecordType;

public class Creator implements Serializable {
    private static final long serialVersionUID = 1L;

    public static final Creator DEFAULT_CREATOR =
            new Creator(MetadataConstants.DEFAULT_CREATOR, MetadataConstants.DEFAULT_CREATOR_UUID);
    private final String name;
    private final String uuid;

    private Creator(String name, String uuid) {
        this.name = name;
        this.uuid = uuid;
    }

    public static Creator create(String name, String uuid) {
        return new Creator(name, uuid);
    }

    public String getName() {
        return name;
    }

    public String getUuid() {
        return uuid;
    }

    @Override
    public String toString() {
        return String.format("Creator{name='%s', uuid='%s'}", name, uuid);
    }

    public static Creator createOrDefault(ARecord record) {
        ARecordType recType = record.getType();
        int creatorIndex = recType.getFieldIndex(MetadataRecordTypes.CREATOR_ARECORD_FIELD_NAME);
        String name = null, uuid = null;

        if (creatorIndex >= 0) {
            ARecordType creatorType = (ARecordType) recType.getFieldTypes()[creatorIndex];
            ARecord creatorRecord = (ARecord) record.getValueByPos(creatorIndex);
            int nameIndex = creatorType.getFieldIndex(MetadataRecordTypes.FIELD_NAME_CREATOR_NAME);
            int uuidIndex = creatorType.getFieldIndex(MetadataRecordTypes.FIELD_NAME_CREATOR_UUID);

            if (nameIndex >= 0) {
                name = ((AString) creatorRecord.getValueByPos(nameIndex)).getStringValue();
            }
            if (uuidIndex >= 0) {
                uuid = ((AString) creatorRecord.getValueByPos(uuidIndex)).getStringValue();
            }
            return create(name, uuid);
        }
        return DEFAULT_CREATOR;
    }

}
