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

import static org.apache.asterix.metadata.bootstrap.MetadataPrimaryIndexes.PROPERTIES_FULL_TEXT_FILTER;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_DATABASE_NAME;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_DATAVERSE_NAME;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_FULL_TEXT_FILTER_NAME;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_FULL_TEXT_FILTER_TYPE;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.RECORD_NAME_FULL_TEXT_FILTER;

import java.util.Arrays;
import java.util.List;

import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;

public final class FullTextFilterEntity {

    private static final FullTextFilterEntity FULL_TEXT_CONFIG = new FullTextFilterEntity(
            new MetadataIndex(PROPERTIES_FULL_TEXT_FILTER, 3, new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING },
                    Arrays.asList(List.of(FIELD_NAME_DATAVERSE_NAME), List.of(FIELD_NAME_FULL_TEXT_FILTER_NAME)), 0,
                    fullTextFilterType(), true, new int[] { 0, 1 }),
            2, -1);

    private static final FullTextFilterEntity DB_FULL_TEXT_CONFIG =
            new FullTextFilterEntity(new MetadataIndex(PROPERTIES_FULL_TEXT_FILTER, 4,
                    new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING },
                    Arrays.asList(List.of(FIELD_NAME_DATABASE_NAME), List.of(FIELD_NAME_DATAVERSE_NAME),
                            List.of(FIELD_NAME_FULL_TEXT_FILTER_NAME)),
                    0, databaseFullTextFilterType(), true, new int[] { 0, 1, 2 }), 3, 0);

    private final int payloadPosition;
    private final MetadataIndex index;
    private final int databaseNameIndex;
    private final int dataverseNameIndex;
    private final int filterNameIndex;
    private final int filterTypeIndex;

    private FullTextFilterEntity(MetadataIndex index, int payloadPosition, int startIndex) {
        this.index = index;
        this.payloadPosition = payloadPosition;
        this.databaseNameIndex = startIndex++;
        this.dataverseNameIndex = startIndex++;
        this.filterNameIndex = startIndex++;
        this.filterTypeIndex = startIndex++;
    }

    public static FullTextFilterEntity of(boolean usingDatabase) {
        return usingDatabase ? DB_FULL_TEXT_CONFIG : FULL_TEXT_CONFIG;
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

    public int filterNameIndex() {
        return filterNameIndex;
    }

    public int filterTypeIndex() {
        return filterTypeIndex;
    }

    private static ARecordType fullTextFilterType() {
        return MetadataRecordTypes.createRecordType(RECORD_NAME_FULL_TEXT_FILTER,
                new String[] { FIELD_NAME_DATAVERSE_NAME, FIELD_NAME_FULL_TEXT_FILTER_NAME,
                        FIELD_NAME_FULL_TEXT_FILTER_TYPE },
                new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING }, true);
    }

    private static ARecordType databaseFullTextFilterType() {
        return MetadataRecordTypes.createRecordType(RECORD_NAME_FULL_TEXT_FILTER,
                new String[] { FIELD_NAME_DATABASE_NAME, FIELD_NAME_DATAVERSE_NAME, FIELD_NAME_FULL_TEXT_FILTER_NAME,
                        FIELD_NAME_FULL_TEXT_FILTER_TYPE },
                new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING },
                true);
    }
}
