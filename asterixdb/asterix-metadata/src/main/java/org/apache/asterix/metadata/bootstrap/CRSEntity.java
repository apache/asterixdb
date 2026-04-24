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

import static org.apache.asterix.metadata.bootstrap.MetadataPrimaryIndexes.PROPERTIES_CRS;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_CRS_NAME;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_CRS_SRID;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_CRS_WKT;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_DATABASE_NAME;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_DATAVERSE_NAME;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.RECORD_NAME_CRS;

import java.util.List;

import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;

public final class CRSEntity {

    private static final CRSEntity CRS =
            new CRSEntity(new MetadataIndex(PROPERTIES_CRS, 3, new IAType[] { BuiltinType.ASTRING, BuiltinType.AINT32 },
                    List.of(List.of(FIELD_NAME_DATAVERSE_NAME), List.of(FIELD_NAME_CRS_SRID)), 0, crsType(), true,
                    new int[] { 0, 1 }), 2, -1);

    private static final CRSEntity DB_CRS =
            new CRSEntity(
                    new MetadataIndex(PROPERTIES_CRS, 4,
                            new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.AINT32 },
                            List.of(List.of(FIELD_NAME_DATABASE_NAME), List.of(FIELD_NAME_DATAVERSE_NAME),
                                    List.of(FIELD_NAME_CRS_SRID)),
                            0, databaseCrsType(), true, new int[] { 0, 1, 2 }),
                    3, 0);

    private final int payloadPosition;
    private final MetadataIndex index;
    private final int databaseNameIndex;
    private final int dataverseNameIndex;
    private final int sridIndex;
    private final int crsNameIndex;
    private final int crsWktIndex;

    private CRSEntity(MetadataIndex index, int payloadPosition, int startIndex) {
        this.index = index;
        this.payloadPosition = payloadPosition;
        this.databaseNameIndex = startIndex++;
        this.dataverseNameIndex = startIndex++;
        this.sridIndex = startIndex++;
        this.crsNameIndex = startIndex++;
        this.crsWktIndex = startIndex++;
    }

    public static CRSEntity of(boolean usingDatabase) {
        return usingDatabase ? DB_CRS : CRS;
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

    public int getSridIndex() {
        return sridIndex;
    }

    public int getCrsNameIndex() {
        return crsNameIndex;
    }

    public int getCrsWktIndex() {
        return crsWktIndex;
    }

    private static ARecordType crsType() {
        return MetadataRecordTypes.createRecordType(RECORD_NAME_CRS,
                new String[] { FIELD_NAME_DATAVERSE_NAME, FIELD_NAME_CRS_SRID, FIELD_NAME_CRS_NAME,
                        FIELD_NAME_CRS_WKT },
                new IAType[] { BuiltinType.ASTRING, BuiltinType.AINT32, BuiltinType.ASTRING, BuiltinType.ASTRING },
                true);
    }

    private static ARecordType databaseCrsType() {
        return MetadataRecordTypes.createRecordType(RECORD_NAME_CRS,
                new String[] { FIELD_NAME_DATABASE_NAME, FIELD_NAME_DATAVERSE_NAME, FIELD_NAME_CRS_SRID,
                        FIELD_NAME_CRS_NAME, FIELD_NAME_CRS_WKT },
                new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.AINT32, BuiltinType.ASTRING,
                        BuiltinType.ASTRING },
                true);
    }
}
