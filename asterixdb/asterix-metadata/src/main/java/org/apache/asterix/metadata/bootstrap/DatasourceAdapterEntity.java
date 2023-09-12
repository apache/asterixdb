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

import static org.apache.asterix.metadata.bootstrap.MetadataPrimaryIndexes.PROPERTIES_DATASOURCE_ADAPTER;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_CLASSNAME;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_DATABASE_NAME;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_DATAVERSE_NAME;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_NAME;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_TIMESTAMP;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_TYPE;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.RECORD_NAME_DATASOURCE_ADAPTER;

import java.util.Arrays;
import java.util.List;

import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;

public final class DatasourceAdapterEntity {

    private static final DatasourceAdapterEntity DATASOURCE_ADAPTER =
            new DatasourceAdapterEntity(new MetadataIndex(PROPERTIES_DATASOURCE_ADAPTER, 3,
                    new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING },
                    Arrays.asList(List.of(FIELD_NAME_DATAVERSE_NAME), List.of(FIELD_NAME_NAME)), 0,
                    datasourceAdapterType(), true, new int[] { 0, 1 }), 2, -1);

    private static final DatasourceAdapterEntity DB_DATASOURCE_ADAPTER = new DatasourceAdapterEntity(
            new MetadataIndex(PROPERTIES_DATASOURCE_ADAPTER, 4,
                    new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING },
                    Arrays.asList(List.of(FIELD_NAME_DATABASE_NAME), List.of(FIELD_NAME_DATAVERSE_NAME),
                            List.of(FIELD_NAME_NAME)),
                    0, databaseDatasourceAdapterType(), true, new int[] { 0, 1, 2 }),
            3, 0);

    private final int payloadPosition;
    private final MetadataIndex index;
    private final int databaseNameIndex;
    private final int dataverseNameIndex;
    private final int adapterNameIndex;
    private final int classNameIndex;
    private final int typeIndex;
    private final int timestampIndex;

    private DatasourceAdapterEntity(MetadataIndex index, int payloadPosition, int startIndex) {
        this.index = index;
        this.payloadPosition = payloadPosition;
        this.databaseNameIndex = startIndex++;
        this.dataverseNameIndex = startIndex++;
        this.adapterNameIndex = startIndex++;
        this.classNameIndex = startIndex++;
        this.typeIndex = startIndex++;
        this.timestampIndex = startIndex++;
    }

    public static DatasourceAdapterEntity of(boolean cloudDeployment) {
        return DATASOURCE_ADAPTER;
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

    public int adapterNameIndex() {
        return adapterNameIndex;
    }

    public int classNameIndex() {
        return classNameIndex;
    }

    public int typeIndex() {
        return typeIndex;
    }

    public int timestampIndex() {
        return timestampIndex;
    }

    private static ARecordType datasourceAdapterType() {
        return MetadataRecordTypes.createRecordType(
                // RecordTypeName
                RECORD_NAME_DATASOURCE_ADAPTER,
                // FieldNames
                new String[] { FIELD_NAME_DATAVERSE_NAME, FIELD_NAME_NAME, FIELD_NAME_CLASSNAME, FIELD_NAME_TYPE,
                        FIELD_NAME_TIMESTAMP },
                // FieldTypes
                new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING,
                        BuiltinType.ASTRING },
                //IsOpen?
                true);
    }

    private static ARecordType databaseDatasourceAdapterType() {
        return MetadataRecordTypes.createRecordType(
                // RecordTypeName
                RECORD_NAME_DATASOURCE_ADAPTER,
                // FieldNames
                new String[] { FIELD_NAME_DATABASE_NAME, FIELD_NAME_DATAVERSE_NAME, FIELD_NAME_NAME,
                        FIELD_NAME_CLASSNAME, FIELD_NAME_TYPE, FIELD_NAME_TIMESTAMP },
                // FieldTypes
                new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING,
                        BuiltinType.ASTRING, BuiltinType.ASTRING },
                //IsOpen?
                true);
    }
}
