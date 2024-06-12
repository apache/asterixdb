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
package org.apache.asterix.external.writer.printer.parquet;

import static org.apache.asterix.external.writer.printer.parquet.ParquetSchemaBuilderUtils.getGroupChild;
import static org.apache.asterix.external.writer.printer.parquet.ParquetSchemaBuilderUtils.getListChild;
import static org.apache.asterix.external.writer.printer.parquet.ParquetSchemaBuilderUtils.getPrimitiveChild;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.IATypeVisitor;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;

public class SchemaConverterVisitor implements IATypeVisitor<Void, Pair<Types.Builder, String>> {
    public static String MESSAGE_NAME = "asterix_schema";
    private final ARecordType schemaType;
    private ATypeTag unsupportedType;

    private SchemaConverterVisitor(ARecordType schemaType) {
        this.schemaType = schemaType;
        this.unsupportedType = null;
    }

    public static String convertToParquetSchemaString(ARecordType schemaType) throws CompilationException {
        SchemaConverterVisitor schemaConverterVisitor = new SchemaConverterVisitor(schemaType);
        return schemaConverterVisitor.getParquetSchema().toString();
    }

    private MessageType getParquetSchema() throws CompilationException {
        Types.MessageTypeBuilder builder = Types.buildMessage();

        for (int i = 0; i < schemaType.getFieldNames().length; i++) {
            String fieldName = schemaType.getFieldNames()[i];
            IAType childType = schemaType.getFieldType(fieldName);
            childType.accept(this, new Pair<>(builder, fieldName));
            if (unsupportedType != null) {
                throw new CompilationException(ErrorCode.TYPE_UNSUPPORTED_PARQUET_WRITE, unsupportedType.toString());
            }
        }
        return builder.named(MESSAGE_NAME);
    }

    @Override
    public Void visit(ARecordType recordType, Pair<Types.Builder, String> arg) {
        Types.Builder builder = arg.first;
        String fieldName = arg.second;

        Types.BaseGroupBuilder childBuilder = getGroupChild(builder);
        for (int i = 0; i < recordType.getFieldNames().length; i++) {
            String childFieldName = recordType.getFieldNames()[i];
            IAType childType = recordType.getFieldType(childFieldName);

            childType.accept(this, new Pair<>(childBuilder, childFieldName));

        }
        childBuilder.named(fieldName);

        return null;
    }

    @Override
    public Void visit(AbstractCollectionType collectionType, Pair<Types.Builder, String> arg) {
        Types.Builder builder = arg.first;
        String fieldName = arg.second;

        Types.BaseListBuilder childBuilder = getListChild(builder);
        IAType child = collectionType.getItemType();
        child.accept(this, new Pair<>(childBuilder, fieldName));

        return null;
    }

    @Override
    public Void visit(AUnionType unionType, Pair<Types.Builder, String> arg) {
        // Shouldn't reach here.
        return null;
    }

    @Override
    public Void visitFlat(IAType flatType, Pair<Types.Builder, String> arg) {
        Types.Builder builder = arg.first;
        String fieldName = arg.second;

        PrimitiveType.PrimitiveTypeName primitiveTypeName =
                AsterixParquetTypeMap.PRIMITIVE_TYPE_NAME_MAP.get(flatType.getTypeTag());

        if (primitiveTypeName == null) {
            unsupportedType = flatType.getTypeTag();
        }

        LogicalTypeAnnotation logicalTypeAnnotation =
                AsterixParquetTypeMap.LOGICAL_TYPE_ANNOTATION_MAP.get(flatType.getTypeTag());

        getPrimitiveChild(builder, primitiveTypeName, logicalTypeAnnotation).named(fieldName);

        return null;
    }

}
