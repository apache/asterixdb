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

import static org.apache.asterix.external.input.record.reader.hdfs.parquet.converter.primitve.PrimitiveConverterProvider.MISSING;
import static org.apache.asterix.om.utils.ProjectionFiltrationTypeUtil.ALL_FIELDS_TYPE;
import static org.apache.asterix.om.utils.ProjectionFiltrationTypeUtil.EMPTY_TYPE;

import java.util.Map;

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.external.input.record.reader.hdfs.parquet.converter.ParquetConverterContext;
import org.apache.asterix.external.input.record.reader.hdfs.parquet.converter.nested.AbstractComplexConverter;
import org.apache.asterix.external.input.record.reader.hdfs.parquet.converter.primitve.DecimalConverter;
import org.apache.asterix.external.util.ExternalDataConstants.ParquetOptions;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.IATypeVisitor;
import org.apache.asterix.runtime.projection.FunctionCallInformation;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.api.exceptions.Warning;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DateLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.IntLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimestampLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.UUIDLogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

/**
 * This visitor clips the filesSchema stored in Parquet using the expected type
 */
public class AsterixTypeToParquetTypeVisitor implements IATypeVisitor<Type, Type> {
    public static final MessageType EMPTY_PARQUET_MESSAGE = Types.buildMessage().named("EMPTY");

    private final ParquetConverterContext context;
    private Map<String, FunctionCallInformation> funcInfo;

    public AsterixTypeToParquetTypeVisitor(ParquetConverterContext context) {
        this.context = context;
    }

    public MessageType clipType(ARecordType rootType, MessageType fileSchema,
            Map<String, FunctionCallInformation> funcInfo) {
        if (rootType == EMPTY_TYPE) {
            return EMPTY_PARQUET_MESSAGE;
        } else if (rootType == ALL_FIELDS_TYPE) {
            return fileSchema;
        }
        Types.MessageTypeBuilder builder = Types.buildMessage();
        this.funcInfo = funcInfo;
        clipObjectChildren(builder, rootType, fileSchema);
        return builder.named(fileSchema.getName());
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
     * {@link AbstractComplexConverter} for more information.
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
            return handleAvroArray(collectionType, arrayType);
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

    private Type handleAvroArray(AbstractCollectionType collectionType, GroupType groupType) {
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
        //typeName is unique
        FunctionCallInformation info = funcInfo.get(node.getTypeName());
        ATypeTag actualType = mapType(type, context, info.getSourceLocation());
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

    /* ****************************************
     * Type checking methods
     * ****************************************
     */

    public static ATypeTag mapType(Type parquetType, ParquetConverterContext context, SourceLocation sourceLocation) {
        LogicalTypeAnnotation typeAnnotation = parquetType.getLogicalTypeAnnotation();
        if (!parquetType.isPrimitive()) {
            if (typeAnnotation == null) {
                return ATypeTag.OBJECT;
            } else if (typeAnnotation == LogicalTypeAnnotation.listType()
                    || typeAnnotation == LogicalTypeAnnotation.mapType()) {
                return ATypeTag.ARRAY;
            }
        } else {
            //Check other primitive types
            PrimitiveType primitiveType = parquetType.asPrimitiveType();
            switch (primitiveType.getPrimitiveTypeName()) {
                case BOOLEAN:
                    return ATypeTag.BOOLEAN;
                case FLOAT:
                case DOUBLE:
                    return ATypeTag.DOUBLE;
                case INT32:
                case INT64:
                    return handleInt32Int64(primitiveType, context, sourceLocation);
                case INT96:
                    return ATypeTag.DATETIME;
                case BINARY:
                case FIXED_LEN_BYTE_ARRAY:
                    return handleBinary(primitiveType, context, sourceLocation);
            }
        }

        warnUnsupportedType(context, sourceLocation, parquetType);
        //Use SYSTEM_NULL for unsupported types
        return ATypeTag.SYSTEM_NULL;
    }

    private static Type getType(GroupType groupType, String fieldName) {
        if (groupType.containsField(fieldName)) {
            return groupType.getType(fieldName);
        }
        return MISSING;
    }

    private static ATypeTag handleInt32Int64(PrimitiveType type, ParquetConverterContext context,
            SourceLocation sourceLocation) {
        LogicalTypeAnnotation logicalType = type.getLogicalTypeAnnotation();
        ATypeTag inferredTypeTag = ATypeTag.SYSTEM_NULL;
        if (logicalType == null || logicalType instanceof IntLogicalTypeAnnotation) {
            inferredTypeTag = ATypeTag.BIGINT;
        } else if (logicalType instanceof DateLogicalTypeAnnotation) {
            inferredTypeTag = ATypeTag.DATE;
        } else if (logicalType instanceof TimeLogicalTypeAnnotation) {
            inferredTypeTag = ATypeTag.TIME;
        } else if (logicalType instanceof TimestampLogicalTypeAnnotation
                && checkDatetime(type, context, sourceLocation)) {
            TimestampLogicalTypeAnnotation tsType = (TimestampLogicalTypeAnnotation) logicalType;
            warnIfUTCAdjustedAndZoneIdIsNotSet(context, sourceLocation, tsType.isAdjustedToUTC());
            inferredTypeTag = ATypeTag.DATETIME;
        } else if (logicalType instanceof DecimalLogicalTypeAnnotation) {
            ensureDecimalToDoubleEnabled(type, context, sourceLocation);
            inferredTypeTag = ATypeTag.DOUBLE;
        }

        //Unsupported type
        return inferredTypeTag;
    }

    private static ATypeTag handleBinary(PrimitiveType type, ParquetConverterContext context,
            SourceLocation sourceLocation) {
        LogicalTypeAnnotation logicalType = type.getLogicalTypeAnnotation();
        ATypeTag inferredTypeTag = ATypeTag.SYSTEM_NULL;
        if (logicalType == null || logicalType == LogicalTypeAnnotation.bsonType()) {
            inferredTypeTag = ATypeTag.BINARY;
        } else if (logicalType == LogicalTypeAnnotation.stringType()
                || logicalType == LogicalTypeAnnotation.enumType()) {
            inferredTypeTag = ATypeTag.STRING;
        } else if (logicalType == LogicalTypeAnnotation.jsonType()) {
            //Parsing JSON could be of any type. if parseJson is disabled, return as String
            inferredTypeTag = context.isParseJsonEnabled() ? ATypeTag.ANY : ATypeTag.STRING;
        } else if (logicalType instanceof DecimalLogicalTypeAnnotation) {
            ensureDecimalToDoubleEnabled(type, context, sourceLocation);
            inferredTypeTag = ATypeTag.DOUBLE;
        } else if (logicalType instanceof UUIDLogicalTypeAnnotation) {
            inferredTypeTag = ATypeTag.UUID;
        }

        //Unsupported type
        return inferredTypeTag;
    }

    private static boolean checkDatetime(PrimitiveType type, ParquetConverterContext context,
            SourceLocation sourceLocation) {
        if (type.getPrimitiveTypeName() == PrimitiveTypeName.INT32) {
            //Only INT64 and INT96 are supported per parquet specification
            warnUnsupportedType(context, sourceLocation, type);
            return false;
        }
        return true;
    }

    private static void ensureDecimalToDoubleEnabled(PrimitiveType type, ParquetConverterContext context,
            SourceLocation sourceLocation) {
        if (!context.isDecimalToDoubleEnabled()) {
            throw new AsterixParquetRuntimeException(
                    new RuntimeDataException(ErrorCode.PARQUET_SUPPORTED_TYPE_WITH_OPTION, sourceLocation,
                            type.toString(), ParquetOptions.DECIMAL_TO_DOUBLE));
        }

        DecimalLogicalTypeAnnotation decimalLogicalType =
                (DecimalLogicalTypeAnnotation) type.getLogicalTypeAnnotation();
        int precision = decimalLogicalType.getPrecision();
        if (precision > DecimalConverter.LONG_MAX_PRECISION) {
            context.getWarnings().add(Warning.of(null, ErrorCode.PARQUET_DECIMAL_TO_DOUBLE_PRECISION_LOSS, precision,
                    DecimalConverter.LONG_MAX_PRECISION));
        }
    }

    public static void warnUnsupportedType(ParquetConverterContext context, SourceLocation sourceLocation,
            Type parquetType) {
        context.getWarnings()
                .add(Warning.of(sourceLocation, ErrorCode.UNSUPPORTED_PARQUET_TYPE, parquetType.toString()));
    }

    private static void warnIfUTCAdjustedAndZoneIdIsNotSet(ParquetConverterContext context,
            SourceLocation sourceLocation, boolean adjustedToUTC) {
        if (adjustedToUTC && context.getTimeZoneId().isEmpty()) {
            Warning warning = Warning.of(sourceLocation, ErrorCode.PARQUET_TIME_ZONE_ID_IS_NOT_SET, ATypeTag.DATETIME,
                    ParquetOptions.TIMEZONE);
            context.getWarnings().add(warning);
        }
    }
}
