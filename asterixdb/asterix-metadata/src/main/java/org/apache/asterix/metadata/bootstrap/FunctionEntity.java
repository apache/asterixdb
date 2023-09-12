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

import static org.apache.asterix.metadata.bootstrap.MetadataPrimaryIndexes.PROPERTIES_FUNCTION;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_ARITY;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_DATABASE_NAME;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_DATAVERSE_NAME;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_DEFINITION;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_DEPENDENCIES;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_KIND;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_LANGUAGE;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_NAME;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_PARAMS;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_RETURN_TYPE;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.RECORD_NAME_FUNCTION;

import java.util.Arrays;
import java.util.List;

import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;

public final class FunctionEntity {

    private static final FunctionEntity FUNCTION = new FunctionEntity(new MetadataIndex(PROPERTIES_FUNCTION, 4,
            new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING },
            Arrays.asList(List.of(FIELD_NAME_DATAVERSE_NAME), List.of(FIELD_NAME_NAME), List.of(FIELD_NAME_ARITY)), 0,
            functionType(), true, new int[] { 0, 1, 2 }), 3, -1);

    private static final FunctionEntity DB_FUNCTION = new FunctionEntity(new MetadataIndex(PROPERTIES_FUNCTION, 5,
            new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING },
            Arrays.asList(List.of(FIELD_NAME_DATABASE_NAME), List.of(FIELD_NAME_DATAVERSE_NAME),
                    List.of(FIELD_NAME_NAME), List.of(FIELD_NAME_ARITY)),
            0, databaseFunctionType(), true, new int[] { 0, 1, 2, 3 }), 4, 0);

    private final int payloadPosition;
    private final MetadataIndex index;
    private final int databaseNameIndex;
    private final int dataverseNameIndex;
    private final int functionNameIndex;
    private final int functionArityIndex;
    private final int functionParamListIndex;
    private final int functionReturnTypeIndex;
    private final int functionDefinitionIndex;
    private final int functionLanguageIndex;
    private final int functionKindIndex;
    private final int functionDependenciesIndex;

    private FunctionEntity(MetadataIndex index, int payloadPosition, int startIndex) {
        this.index = index;
        this.payloadPosition = payloadPosition;
        this.databaseNameIndex = startIndex++;
        this.dataverseNameIndex = startIndex++;
        this.functionNameIndex = startIndex++;
        this.functionArityIndex = startIndex++;
        this.functionParamListIndex = startIndex++;
        this.functionReturnTypeIndex = startIndex++;
        this.functionDefinitionIndex = startIndex++;
        this.functionLanguageIndex = startIndex++;
        this.functionKindIndex = startIndex++;
        this.functionDependenciesIndex = startIndex++;
    }

    public static FunctionEntity of(boolean cloudDeployment) {
        return FUNCTION;
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

    public int functionNameIndex() {
        return functionNameIndex;
    }

    public int functionArityIndex() {
        return functionArityIndex;
    }

    public int functionParamListIndex() {
        return functionParamListIndex;
    }

    public int functionReturnTypeIndex() {
        return functionReturnTypeIndex;
    }

    public int functionLanguageIndex() {
        return functionLanguageIndex;
    }

    public int functionDefinitionIndex() {
        return functionDefinitionIndex;
    }

    public int functionKindIndex() {
        return functionKindIndex;
    }

    public int functionDependenciesIndex() {
        return functionDependenciesIndex;
    }

    private static ARecordType functionType() {
        return MetadataRecordTypes.createRecordType(
                // RecordTypeName
                RECORD_NAME_FUNCTION,
                // FieldNames
                new String[] { FIELD_NAME_DATAVERSE_NAME, FIELD_NAME_NAME, FIELD_NAME_ARITY, FIELD_NAME_PARAMS,
                        FIELD_NAME_RETURN_TYPE, FIELD_NAME_DEFINITION, FIELD_NAME_LANGUAGE, FIELD_NAME_KIND,
                        FIELD_NAME_DEPENDENCIES },
                // FieldTypes
                new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING,
                        new AOrderedListType(BuiltinType.ASTRING, null), BuiltinType.ASTRING, BuiltinType.ASTRING,
                        BuiltinType.ASTRING, BuiltinType.ASTRING,
                        new AOrderedListType(
                                new AOrderedListType(new AOrderedListType(BuiltinType.ASTRING, null), null), null) },
                //IsOpen?
                true);
    }

    private static ARecordType databaseFunctionType() {
        return MetadataRecordTypes.createRecordType(
                // RecordTypeName
                RECORD_NAME_FUNCTION,
                // FieldNames
                new String[] { FIELD_NAME_DATABASE_NAME, FIELD_NAME_DATAVERSE_NAME, FIELD_NAME_NAME, FIELD_NAME_ARITY,
                        FIELD_NAME_PARAMS, FIELD_NAME_RETURN_TYPE, FIELD_NAME_DEFINITION, FIELD_NAME_LANGUAGE,
                        FIELD_NAME_KIND, FIELD_NAME_DEPENDENCIES },
                // FieldTypes
                new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING,
                        new AOrderedListType(BuiltinType.ASTRING, null), BuiltinType.ASTRING, BuiltinType.ASTRING,
                        BuiltinType.ASTRING, BuiltinType.ASTRING,
                        new AOrderedListType(
                                new AOrderedListType(new AOrderedListType(BuiltinType.ASTRING, null), null), null) },
                //IsOpen?
                true);
    }
}
