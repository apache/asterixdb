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
package org.apache.asterix.external.input.record.reader.hdfs.parquet;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.IATypeVisitor;
import org.apache.asterix.runtime.projection.DataProjectionInfo;
import org.apache.asterix.runtime.projection.FunctionCallInformation;
import org.apache.hyracks.api.exceptions.Warning;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

/**
 * This visitor clips the filesSchema stored in Parquet using the expected type
 */
public class AsterixTypeToParquetTypeVisitor implements IATypeVisitor<Type, Type> {
    public static final MessageType EMPTY_PARQUET_MESSAGE = Types.buildMessage().named("EMPTY");
    public static final PrimitiveType MISSING =
            Types.optional(PrimitiveType.PrimitiveTypeName.BOOLEAN).named("MISSING");

    private final List<Warning> warnings;
    private Map<String, FunctionCallInformation> funcInfo;

    public AsterixTypeToParquetTypeVisitor(boolean shouldWarn) {
        warnings = shouldWarn ? new ArrayList<>() : null;
    }

    public MessageType clipType(ARecordType rootType, MessageType fileSchema,
            Map<String, FunctionCallInformation> funcInfo) {
        if (rootType == DataProjectionInfo.EMPTY_TYPE) {
            return EMPTY_PARQUET_MESSAGE;
        } else if (rootType == DataProjectionInfo.ALL_FIELDS_TYPE) {
            return fileSchema;
        }
        Types.MessageTypeBuilder builder = Types.buildMessage();
        this.funcInfo = funcInfo;
        clipObjectChildren(builder, rootType, fileSchema);
        return builder.named(fileSchema.getName());
    }

    public List<Warning> getWarnings() {
        return warnings;
    }

    @Override
    public Type visit(ARecordType recordType, Type arg) {
        //No LogicalTypeAnnotation for Object types
        if (isNotCompatibleType(arg, recordType)) {
            return MISSING;
        }
        Types.GroupBuilder<GroupType> builder = Types.buildGroup(arg.getRepetition());
        if (clipObjectChildren(builder, recordType, arg) == 0) {
            //If no fields where added, add MISSING as parquet does not allow empty objects.
            builder.addField(MISSING);
        }
        return builder.named(arg.getName());
    }

    /**
     * There are two ways for representing arrays using ProtoBuf schema see the example in
     * {@link org.apache.asterix.external.input.record.reader.hdfs.parquet.AbstractComplexConverter} for more
     * information.
     */
    @Override
    public Type visit(AbstractCollectionType collectionType, Type arg) {
        if (isNotCompatibleType(arg, collectionType)) {
            return MISSING;
        }
        GroupType arrayType = arg.asGroupType();
        //There is only one child
        Type childType = arrayType.getType(0);
        if ("array".equals(childType.getName()) || childType.asGroupType().getFieldCount() > 1) {
            //Handle Avro-like schema
            return handleHandleAvroArray(collectionType, arrayType);
        }
        //Handling spark-like schema
        Types.ListBuilder<GroupType> builder = Types.list(arg.getRepetition());
        //LIST items always wrapped in a group called list
        childType = childType.asGroupType().getType(0);
        Type requestedChildType = collectionType.getItemType().accept(this, childType);
        //setElementType will wrap childType again in a group type
        builder.setElementType(requestedChildType);
        return builder.named(arg.getName());
    }

    private int clipObjectChildren(Types.GroupBuilder<?> builder, ARecordType recordType, Type arg) {
        GroupType groupType = arg.asGroupType();
        String[] fieldNames = recordType.getFieldNames();
        IAType[] fieldTypes = recordType.getFieldTypes();
        int numberOfAddedFields = 0;
        for (int i = 0; i < fieldNames.length; i++) {
            Type type = getType(groupType, fieldNames[i]);
            Type childType = fieldTypes[i].accept(this, type);
            if (childType != MISSING) {
                //We only add non-MISSING children
                builder.addField(childType);
                numberOfAddedFields++;
            }
        }
        return numberOfAddedFields;
    }

    private Type handleHandleAvroArray(AbstractCollectionType collectionType, GroupType groupType) {
        Types.GroupBuilder<GroupType> builder =
                Types.buildGroup(groupType.getRepetition()).as(groupType.getLogicalTypeAnnotation());
        //There is only one child
        Type type = groupType.getType(0);
        Type childType = collectionType.getItemType().accept(this, type);
        builder.addField(childType);
        return builder.named(groupType.getName());
    }

    @Override
    public Type visit(AUnionType unionType, Type arg) {
        if (arg.getLogicalTypeAnnotation() == LogicalTypeAnnotation.listType()) {
            //Currently, only array type is supported
            return unionType.getType(ATypeTag.ARRAY).accept(this, arg);
        } else {
            return unionType.getType(ATypeTag.OBJECT).accept(this, arg);
        }
    }

    @Override
    public Type visitFlat(IAType node, Type arg) {
        return arg;
    }

    private boolean isNotCompatibleType(Type type, IAType node) {
        if (type == MISSING) {
            return true;
        }
        ATypeTag actualType = mapType(type);
        ATypeTag expectedType = node.getTypeTag();

        boolean isNotExpected = actualType != expectedType;
        if (warnings != null && isNotExpected) {
            //typeName is unique
            FunctionCallInformation info = funcInfo.get(node.getTypeName());
            //If no warning is created, then it means it has been reported
            Warning warning = info.createTypeMismatchWarning(expectedType, actualType);
            if (warning != null) {
                //New warning that we saw for the first time. We should report it.
                warnings.add(warning);
            }
        }
        return isNotExpected;
    }

    private static ATypeTag mapType(Type parquetType) {
        LogicalTypeAnnotation typeAnnotation = parquetType.getLogicalTypeAnnotation();
        if (!parquetType.isPrimitive() && typeAnnotation == null) {
            return ATypeTag.OBJECT;
        } else if (typeAnnotation == LogicalTypeAnnotation.listType()) {
            return ATypeTag.ARRAY;
        } else if (typeAnnotation == LogicalTypeAnnotation.stringType()) {
            return ATypeTag.STRING;
        } else {
            //Check other primitive types
            PrimitiveType.PrimitiveTypeName primitiveTypeName = parquetType.asPrimitiveType().getPrimitiveTypeName();
            switch (primitiveTypeName) {
                case BOOLEAN:
                    return ATypeTag.BOOLEAN;
                case INT32:
                case INT64:
                    return ATypeTag.BIGINT;
                case FLOAT:
                case DOUBLE:
                    return ATypeTag.DOUBLE;
                default:
                    throw new IllegalStateException("Unsupported type " + parquetType);
            }
        }
    }

    private static Type getType(GroupType groupType, String fieldName) {
        if (groupType.containsField(fieldName)) {
            return groupType.getType(fieldName);
        }
        return MISSING;
    }
}
