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

import org.apache.asterix.om.lazy.AbstractLazyVisitablePointable;
import org.apache.asterix.om.lazy.AbstractListLazyVisitablePointable;
import org.apache.asterix.om.lazy.FlatLazyVisitablePointable;
import org.apache.asterix.om.lazy.ILazyVisitablePointableVisitor;
import org.apache.asterix.om.lazy.RecordLazyVisitablePointable;
import org.apache.asterix.om.lazy.TypedRecordLazyVisitablePointable;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;

public class SchemaCheckerLazyVisitor implements ISchemaChecker,
        ILazyVisitablePointableVisitor<ISchemaChecker.SchemaComparisonType, ParquetSchemaTree.SchemaNode> {
    private final FieldNamesDictionary fieldNamesDictionary;
    private final RecordLazyVisitablePointable record;

    public SchemaCheckerLazyVisitor(IAType typeInfo) {
        this.fieldNamesDictionary = new FieldNamesDictionary();
        if (typeInfo.getTypeTag() == ATypeTag.OBJECT) {
            this.record = new TypedRecordLazyVisitablePointable((ARecordType) typeInfo);
        } else if (typeInfo.getTypeTag() == ATypeTag.ANY) {
            this.record = new RecordLazyVisitablePointable(true);
        } else {
            throw new RuntimeException("Type Unsupported for parquet printing");
        }
    }

    @Override
    public ISchemaChecker.SchemaComparisonType visit(RecordLazyVisitablePointable pointable,
            ParquetSchemaTree.SchemaNode schemaNode) throws HyracksDataException {
        if (schemaNode.getType() == null) {
            return ISchemaChecker.SchemaComparisonType.GROWING;
        }

        if (!(schemaNode.getType() instanceof ParquetSchemaTree.RecordType)) {
            return ISchemaChecker.SchemaComparisonType.CONFLICTING;
        }

        ParquetSchemaTree.RecordType recordType = (ParquetSchemaTree.RecordType) schemaNode.getType();
        ISchemaChecker.SchemaComparisonType schemaComparisonType = ISchemaChecker.SchemaComparisonType.EQUIVALENT;

        for (int i = 0; i < pointable.getNumberOfChildren(); i++) {
            pointable.nextChild();
            AbstractLazyVisitablePointable child = pointable.getChildVisitablePointable();
            String childColumnName = fieldNamesDictionary.getOrCreateFieldNameIndex(pointable.getFieldName());
            ParquetSchemaTree.SchemaNode childType = recordType.getChildren().get(childColumnName);
            if (childType == null) {
                schemaComparisonType =
                        ISchemaChecker.max(schemaComparisonType, ISchemaChecker.SchemaComparisonType.GROWING);
                continue;
            }
            schemaComparisonType = ISchemaChecker.max(schemaComparisonType, child.accept(this, childType));
        }
        return schemaComparisonType;
    }

    @Override
    public ISchemaChecker.SchemaComparisonType visit(AbstractListLazyVisitablePointable pointable,
            ParquetSchemaTree.SchemaNode schemaNode) throws HyracksDataException {
        if (schemaNode.getType() == null) {
            return ISchemaChecker.SchemaComparisonType.GROWING;
        }
        if (!(schemaNode.getType() instanceof ParquetSchemaTree.ListType)) {
            return ISchemaChecker.SchemaComparisonType.CONFLICTING;
        }

        ParquetSchemaTree.ListType listType = (ParquetSchemaTree.ListType) schemaNode.getType();
        ISchemaChecker.SchemaComparisonType schemaComparisonType = ISchemaChecker.SchemaComparisonType.EQUIVALENT;

        for (int i = 0; i < pointable.getNumberOfChildren(); i++) {
            pointable.nextChild();
            AbstractLazyVisitablePointable child = pointable.getChildVisitablePointable();
            if (listType.isEmpty()) {
                schemaComparisonType =
                        ISchemaChecker.max(schemaComparisonType, ISchemaChecker.SchemaComparisonType.GROWING);
                continue;
            }
            schemaComparisonType = ISchemaChecker.max(schemaComparisonType, child.accept(this, listType.getChild()));
        }
        return schemaComparisonType;
    }

    @Override
    public ISchemaChecker.SchemaComparisonType visit(FlatLazyVisitablePointable pointable,
            ParquetSchemaTree.SchemaNode schemaNode) throws HyracksDataException {
        if (schemaNode.getType() == null) {
            return ISchemaChecker.SchemaComparisonType.GROWING;
        }
        if (!(schemaNode.getType() instanceof ParquetSchemaTree.FlatType)) {
            return ISchemaChecker.SchemaComparisonType.CONFLICTING;
        }

        ParquetSchemaTree.FlatType flatType = (ParquetSchemaTree.FlatType) schemaNode.getType();

        if (flatType.getPrimitiveTypeName() != AsterixParquetTypeMap.PRIMITIVE_TYPE_NAME_MAP.get(pointable.getTypeTag())
                || !(flatType.getLogicalTypeAnnotation() == AsterixParquetTypeMap.LOGICAL_TYPE_ANNOTATION_MAP
                        .get(pointable.getTypeTag()))) {
            return ISchemaChecker.SchemaComparisonType.CONFLICTING;
        }

        return ISchemaChecker.SchemaComparisonType.EQUIVALENT;
    }

    @Override
    public SchemaComparisonType checkSchema(ParquetSchemaTree.SchemaNode schemaNode, IValueReference valueReference)
            throws HyracksDataException {
        record.set(valueReference);
        return record.accept(this, schemaNode);
    }
}
