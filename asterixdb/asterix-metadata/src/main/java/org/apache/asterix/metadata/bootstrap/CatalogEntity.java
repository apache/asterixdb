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

import static org.apache.asterix.metadata.bootstrap.MetadataPrimaryIndexes.PROPERTIES_CATALOG;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.CATALOG_DETAILS_RECORDTYPE;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_CATALOG_DETAILS;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_CATALOG_NAME;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_CATALOG_TYPE;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_PENDING_OP;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_TIMESTAMP;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.RECORD_NAME_CATALOG;

import java.util.List;

import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;

public final class CatalogEntity {

    private static final CatalogEntity CATALOG =
            new CatalogEntity(new MetadataIndex(PROPERTIES_CATALOG, 2, new IAType[] { BuiltinType.ASTRING },
                    List.of(List.of(FIELD_NAME_CATALOG_NAME)), 0, catalogType(), true, new int[] { 0 }), 1, 0);

    private final int payloadPosition;
    private final MetadataIndex index;
    private final int catalogNameIndex;
    private final int catalogTypeIndex;
    private final int catalogDetailsIndex;
    private final int timestampIndex;
    private final int pendingOpIndex;

    private CatalogEntity(MetadataIndex index, int payloadPosition, int startIndex) {
        this.index = index;
        this.payloadPosition = payloadPosition;
        this.catalogNameIndex = startIndex++;
        this.catalogTypeIndex = startIndex++;
        this.catalogDetailsIndex = startIndex++;
        this.timestampIndex = startIndex++;
        this.pendingOpIndex = startIndex++;
    }

    public static CatalogEntity of(boolean cloudDeployment) {
        return CATALOG;
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

    public int getCatalogNameIndex() {
        return catalogNameIndex;
    }

    public int getCatalogTypeIndex() {
        return catalogTypeIndex;
    }

    public int getCatalogDetailsIndex() {
        return catalogDetailsIndex;
    }

    public int getTimestampIndex() {
        return timestampIndex;
    }

    public int getPendingOpIndex() {
        return pendingOpIndex;
    }

    private static ARecordType catalogType() {
        return MetadataRecordTypes.createRecordType(
                // RecordTypeName
                RECORD_NAME_CATALOG,
                // FieldNames
                new String[] { FIELD_NAME_CATALOG_NAME, FIELD_NAME_CATALOG_TYPE, FIELD_NAME_CATALOG_DETAILS,
                        FIELD_NAME_TIMESTAMP, FIELD_NAME_PENDING_OP },
                // FieldTypes
                new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING,
                        AUnionType.createUnknownableType(CATALOG_DETAILS_RECORDTYPE), BuiltinType.ASTRING,
                        BuiltinType.AINT32 },
                //IsOpen?
                true);
    }
}
