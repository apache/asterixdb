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
package org.apache.asterix.external.input.record.reader.aws.delta;

import static org.apache.asterix.om.utils.ProjectionFiltrationTypeUtil.ALL_FIELDS_TYPE;
import static org.apache.asterix.om.utils.ProjectionFiltrationTypeUtil.EMPTY_TYPE;

import java.util.Map;

import org.apache.asterix.external.input.record.reader.aws.delta.converter.DeltaConverterContext;
import org.apache.asterix.external.parser.DeltaDataParser;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.IATypeVisitor;
import org.apache.asterix.runtime.projection.FunctionCallInformation;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.Warning;

import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.StructType;

/**
 * This visitor clips the filesSchema stored in Delta table metadata using the expected type
 */
public class AsterixTypeToDeltaTypeVisitor implements IATypeVisitor<DataType, DataType> {

    private final DeltaConverterContext context;
    private Map<String, FunctionCallInformation> funcInfo;

    public AsterixTypeToDeltaTypeVisitor(DeltaConverterContext context) {
        this.context = context;
    }

    public StructType clipType(ARecordType rootType, StructType fileSchema,
            Map<String, FunctionCallInformation> funcInfo) {
        if (rootType == EMPTY_TYPE) {
            return new StructType();
        } else if (rootType == ALL_FIELDS_TYPE) {
            return fileSchema;
        }
        StructType builder = new StructType();
        this.funcInfo = funcInfo;
        return clipObjectChildren(builder, rootType, fileSchema);
    }

    @Override
    public DataType visit(ARecordType recordType, DataType arg) {
        if (isNotCompatibleType(arg, recordType)) {
            return null;
        }
        StructType builder = new StructType();
        builder = clipObjectChildren(builder, recordType, (StructType) arg);
        if (builder.fields().size() == 0) {
            return null;
        }
        return builder;
    }

    @Override
    public DataType visit(AbstractCollectionType collectionType, DataType arg) {
        if (isNotCompatibleType(arg, collectionType)) {
            return null;
        }
        DataType elementSchema = ((ArrayType) arg).getElementType();
        DataType requestedChildType = collectionType.getItemType().accept(this, elementSchema);
        return new ArrayType(requestedChildType, true);
    }

    private StructType clipObjectChildren(StructType builder, ARecordType recordType, StructType arg) {
        String[] fieldNames = recordType.getFieldNames();
        IAType[] fieldTypes = recordType.getFieldTypes();
        for (int i = 0; i < fieldNames.length; i++) {
            // If the field is not present in the file schema, we skip it
            if (arg.fieldNames().contains(fieldNames[i])) {
                DataType type = arg.get(fieldNames[i]).getDataType();
                DataType childType = fieldTypes[i].accept(this, type);
                if (childType != null) {
                    // We only add non-MISSING children
                    builder = builder.add(fieldNames[i], childType);
                }
            }
        }
        return builder;
    }

    @Override
    public DataType visit(AUnionType unionType, DataType arg) {
        if (arg instanceof ArrayType) {
            return unionType.getType(ATypeTag.ARRAY).accept(this, arg);
        } else {
            return unionType.getType(ATypeTag.OBJECT).accept(this, arg);
        }
    }

    @Override
    public DataType visitFlat(IAType node, DataType arg) {
        return arg;
    }

    private boolean isNotCompatibleType(DataType type, IAType node) {
        // typeName is unique
        FunctionCallInformation info = funcInfo.get(node.getTypeName());
        ATypeTag actualType = null;
        try {
            actualType = DeltaDataParser.getTypeTag(type, false, context);
        } catch (HyracksDataException e) {
            throw new AsterixDeltaRuntimeException(e);
        }
        ATypeTag expectedType = node.getTypeTag();

        boolean isNotExpected = actualType != expectedType;
        if (isNotExpected) {
            //If no warning is created, then it means it has been reported
            Warning warning = null;
            if (actualType != ATypeTag.SYSTEM_NULL) {
                warning = info.createWarning(expectedType, actualType);
            }
            if (warning != null) {
                //New warning that we saw for the first time. We should report it.
                context.getWarnings().add(warning);
            }
        }
        return isNotExpected;
    }
}
