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

import static org.apache.asterix.external.writer.printer.parquet.ParquetValueWriter.ELEMENT_FIELD;
import static org.apache.asterix.external.writer.printer.parquet.ParquetValueWriter.GROUP_TYPE_ERROR_FIELD;
import static org.apache.asterix.external.writer.printer.parquet.ParquetValueWriter.LIST_FIELD;
import static org.apache.asterix.external.writer.printer.parquet.ParquetValueWriter.PRIMITIVE_TYPE_ERROR_FIELD;

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
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

public class ParquetRecordLazyVisitor implements ILazyVisitablePointableVisitor<Void, Type> {

    private final MessageType schema;
    private final RecordLazyVisitablePointable rec;
    private RecordConsumer recordConsumer;
    private final FieldNamesDictionary fieldNamesDictionary;

    private final ParquetValueWriter parquetValueWriter;

    public ParquetRecordLazyVisitor(MessageType schema, IAType typeInfo) {
        this.schema = schema;
        if (typeInfo.getTypeTag() == ATypeTag.OBJECT) {
            this.rec = new TypedRecordLazyVisitablePointable((ARecordType) typeInfo);
        } else if (typeInfo.getTypeTag() == ATypeTag.ANY) {
            this.rec = new RecordLazyVisitablePointable(true);
        } else {
            throw new RuntimeException("Type Unsupported for parquet printing");
        }
        this.fieldNamesDictionary = new FieldNamesDictionary();
        this.parquetValueWriter = new ParquetValueWriter();
    }

    public MessageType getSchema() {
        return schema;
    }

    @Override
    public Void visit(RecordLazyVisitablePointable pointable, Type type) throws HyracksDataException {

        if (type.isPrimitive()) {
            throw new HyracksDataException(ErrorCode.RESULT_DOES_NOT_FOLLOW_SCHEMA, GROUP_TYPE_ERROR_FIELD,
                    PRIMITIVE_TYPE_ERROR_FIELD, type.getName());
        }
        GroupType groupType = type.asGroupType();
        recordConsumer.startGroup();

        for (int i = 0; i < pointable.getNumberOfChildren(); i++) {
            pointable.nextChild();
            AbstractLazyVisitablePointable child = pointable.getChildVisitablePointable();
            String columnName = fieldNamesDictionary.getOrCreateFieldNameIndex(pointable.getFieldName());

            if (!groupType.containsField(columnName)) {
                throw new HyracksDataException(ErrorCode.EXTRA_FIELD_IN_RESULT_NOT_FOUND_IN_SCHEMA, columnName,
                        groupType.getName());
            }
            recordConsumer.startField(columnName, groupType.getFieldIndex(columnName));
            child.accept(this, groupType.getType(columnName));
            recordConsumer.endField(columnName, groupType.getFieldIndex(columnName));
        }
        recordConsumer.endGroup();
        return null;
    }

    @Override
    public Void visit(AbstractListLazyVisitablePointable pointable, Type type) throws HyracksDataException {

        if (type.isPrimitive()) {
            throw new HyracksDataException(ErrorCode.RESULT_DOES_NOT_FOLLOW_SCHEMA, GROUP_TYPE_ERROR_FIELD,
                    PRIMITIVE_TYPE_ERROR_FIELD, type.getName());
        }
        GroupType groupType = type.asGroupType();

        if (!groupType.containsField(LIST_FIELD)) {
            throw new HyracksDataException(ErrorCode.EXTRA_FIELD_IN_RESULT_NOT_FOUND_IN_SCHEMA, LIST_FIELD,
                    groupType.getName());
        }

        if (groupType.getType(LIST_FIELD).isPrimitive()) {
            throw new HyracksDataException(ErrorCode.RESULT_DOES_NOT_FOLLOW_SCHEMA, GROUP_TYPE_ERROR_FIELD,
                    PRIMITIVE_TYPE_ERROR_FIELD, LIST_FIELD);
        }

        GroupType listType = groupType.getType(LIST_FIELD).asGroupType();

        if (!listType.containsField(ELEMENT_FIELD)) {
            throw new HyracksDataException(ErrorCode.EXTRA_FIELD_IN_RESULT_NOT_FOUND_IN_SCHEMA, ELEMENT_FIELD,
                    listType.getName());
        }

        recordConsumer.startGroup();

        if (pointable.getNumberOfChildren() > 0) {
            recordConsumer.startField(LIST_FIELD, groupType.getFieldIndex(LIST_FIELD));

            for (int i = 0; i < pointable.getNumberOfChildren(); i++) {
                pointable.nextChild();
                AbstractLazyVisitablePointable child = pointable.getChildVisitablePointable();

                recordConsumer.startGroup();
                recordConsumer.startField(ELEMENT_FIELD, listType.getFieldIndex(ELEMENT_FIELD));
                child.accept(this, listType.getType(ELEMENT_FIELD));
                recordConsumer.endField(ELEMENT_FIELD, listType.getFieldIndex(ELEMENT_FIELD));
                recordConsumer.endGroup();

            }

            recordConsumer.endField(LIST_FIELD, groupType.getFieldIndex(LIST_FIELD));
        }

        recordConsumer.endGroup();
        return null;
    }

    @Override
    public Void visit(FlatLazyVisitablePointable pointable, Type type) throws HyracksDataException {

        if (!type.isPrimitive()) {
            throw new HyracksDataException(ErrorCode.RESULT_DOES_NOT_FOLLOW_SCHEMA, PRIMITIVE_TYPE_ERROR_FIELD,
                    GROUP_TYPE_ERROR_FIELD, type.getName());
        }
        parquetValueWriter.addValueToColumn(recordConsumer, pointable, type.asPrimitiveType());
        return null;
    }

    public void consumeRecord(IValueReference valueReference, RecordConsumer recordConsumer)
            throws HyracksDataException {
        rec.set(valueReference);
        this.recordConsumer = recordConsumer;

        recordConsumer.startMessage();
        for (int i = 0; i < rec.getNumberOfChildren(); i++) {
            rec.nextChild();
            String columnName = fieldNamesDictionary.getOrCreateFieldNameIndex(rec.getFieldName());
            AbstractLazyVisitablePointable child = rec.getChildVisitablePointable();

            if (!schema.containsField(columnName)) {
                throw new HyracksDataException(ErrorCode.EXTRA_FIELD_IN_RESULT_NOT_FOUND_IN_SCHEMA, columnName,
                        schema.getName());
            }

            recordConsumer.startField(columnName, schema.getFieldIndex(columnName));
            child.accept(this, schema.getType(columnName));
            recordConsumer.endField(columnName, schema.getFieldIndex(columnName));
        }
        recordConsumer.endMessage();
    }

}
