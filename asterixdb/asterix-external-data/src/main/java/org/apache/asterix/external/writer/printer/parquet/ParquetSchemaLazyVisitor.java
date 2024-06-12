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

import static org.apache.asterix.external.writer.printer.parquet.ParquetSchemaTree.buildParquetSchema;

import java.util.Map;

import org.apache.asterix.om.lazy.AbstractLazyVisitablePointable;
import org.apache.asterix.om.lazy.AbstractListLazyVisitablePointable;
import org.apache.asterix.om.lazy.FlatLazyVisitablePointable;
import org.apache.asterix.om.lazy.ILazyVisitablePointableVisitor;
import org.apache.asterix.om.lazy.RecordLazyVisitablePointable;
import org.apache.asterix.om.lazy.TypedRecordLazyVisitablePointable;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;

public class ParquetSchemaLazyVisitor implements ILazyVisitablePointableVisitor<Void, ParquetSchemaTree.SchemaNode> {
    private final RecordLazyVisitablePointable rec;
    private final FieldNamesDictionary fieldNamesDictionary;
    private final static String SCHEMA_NAME = "asterix_schema";

    public ParquetSchemaLazyVisitor(IAType typeInfo) {
        this.fieldNamesDictionary = new FieldNamesDictionary();
        if (typeInfo.getTypeTag() == ATypeTag.OBJECT) {
            this.rec = new TypedRecordLazyVisitablePointable((ARecordType) typeInfo);
        } else if (typeInfo.getTypeTag() == ATypeTag.ANY) {
            this.rec = new RecordLazyVisitablePointable(true);
        } else {
            throw new RuntimeException("Type Unsupported for parquet printing");
        }
    }

    @Override
    public Void visit(RecordLazyVisitablePointable pointable, ParquetSchemaTree.SchemaNode schemaNode)
            throws HyracksDataException {
        if (schemaNode.getType() == null) {
            schemaNode.setType(new ParquetSchemaTree.RecordType());
        }
        if (!(schemaNode.getType() instanceof ParquetSchemaTree.RecordType)) {
            throw new HyracksDataException(ErrorCode.RESULT_DOES_NOT_FOLLOW_SCHEMA);
        }
        ParquetSchemaTree.RecordType recordType = (ParquetSchemaTree.RecordType) schemaNode.getType();
        for (int i = 0; i < pointable.getNumberOfChildren(); i++) {
            pointable.nextChild();
            AbstractLazyVisitablePointable child = pointable.getChildVisitablePointable();
            String childColumnName = fieldNamesDictionary.getOrCreateFieldNameIndex(pointable.getFieldName());
            ParquetSchemaTree.SchemaNode childType;
            if (recordType.getChildren().containsKey(childColumnName)) {
                childType = recordType.getChildren().get(childColumnName);
            } else {
                childType = new ParquetSchemaTree.SchemaNode();
                recordType.add(childColumnName, childType);
            }
            child.accept(this, childType);
        }
        return null;
    }

    @Override
    public Void visit(AbstractListLazyVisitablePointable pointable, ParquetSchemaTree.SchemaNode schemaNode)
            throws HyracksDataException {
        if (schemaNode.getType() == null) {
            schemaNode.setType(new ParquetSchemaTree.ListType());
        }
        if (!(schemaNode.getType() instanceof ParquetSchemaTree.ListType)) {
            throw new HyracksDataException(ErrorCode.RESULT_DOES_NOT_FOLLOW_SCHEMA);
        }
        ParquetSchemaTree.ListType listType = (ParquetSchemaTree.ListType) schemaNode.getType();
        for (int i = 0; i < pointable.getNumberOfChildren(); i++) {
            pointable.nextChild();
            AbstractLazyVisitablePointable child = pointable.getChildVisitablePointable();
            if (listType.isEmpty()) {
                listType.setChild(new ParquetSchemaTree.SchemaNode());
            }
            child.accept(this, listType.getChild());
        }
        return null;
    }

    @Override
    public Void visit(FlatLazyVisitablePointable pointable, ParquetSchemaTree.SchemaNode schemaNode)
            throws HyracksDataException {
        if (schemaNode.getType() == null) {
            schemaNode.setType(new ParquetSchemaTree.FlatType(
                    AsterixParquetTypeMap.PRIMITIVE_TYPE_NAME_MAP.get(pointable.getTypeTag()),
                    AsterixParquetTypeMap.LOGICAL_TYPE_ANNOTATION_MAP.get(pointable.getTypeTag())));
            return null;
        }
        if (!(schemaNode.getType() instanceof ParquetSchemaTree.FlatType)) {
            throw new HyracksDataException(ErrorCode.RESULT_DOES_NOT_FOLLOW_SCHEMA);
        }
        ParquetSchemaTree.FlatType flatType = (ParquetSchemaTree.FlatType) schemaNode.getType();
        if (!(flatType.getPrimitiveTypeName() == AsterixParquetTypeMap.PRIMITIVE_TYPE_NAME_MAP
                .get(pointable.getTypeTag()))
                || !(flatType.getLogicalTypeAnnotation() == AsterixParquetTypeMap.LOGICAL_TYPE_ANNOTATION_MAP
                        .get(pointable.getTypeTag()))) {
            throw new HyracksDataException(ErrorCode.RESULT_DOES_NOT_FOLLOW_SCHEMA);
        }
        return null;
    }

    public ParquetSchemaTree.SchemaNode inferSchema(IValueReference valueReference) throws HyracksDataException {
        ParquetSchemaTree.SchemaNode schemaNode = new ParquetSchemaTree.SchemaNode();
        rec.set(valueReference);
        rec.accept(this, schemaNode);
        return schemaNode;
    }

    public static MessageType generateSchema(ParquetSchemaTree.SchemaNode schemaRoot) throws HyracksDataException {
        Types.MessageTypeBuilder builder = Types.buildMessage();
        if (schemaRoot.getType() == null)
            return builder.named(SCHEMA_NAME);
        for (Map.Entry<String, ParquetSchemaTree.SchemaNode> entry : ((ParquetSchemaTree.RecordType) schemaRoot
                .getType()).getChildren().entrySet()) {
            buildParquetSchema(builder, entry.getValue(), entry.getKey());
        }
        return builder.named(SCHEMA_NAME);
    }

}
