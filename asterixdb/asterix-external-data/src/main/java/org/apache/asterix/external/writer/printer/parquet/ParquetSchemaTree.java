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

import java.util.HashMap;
import java.util.Map;

import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;

public class ParquetSchemaTree {

    public static class SchemaNode {
        private AbstractType type;

        public SchemaNode() {
            type = null;
        }

        public void setType(AbstractType type) {
            this.type = type;
        }

        public AbstractType getType() {
            return type;
        }
    }

    static class RecordType extends AbstractType {
        private final Map<String, SchemaNode> children;

        public RecordType() {
            children = new HashMap<>();
        }

        void add(String fieldName, SchemaNode childType) {
            children.put(fieldName, childType);
        }

        SchemaNode get(String fieldName) {
            return children.get(fieldName);
        }

        Map<String, SchemaNode> getChildren() {
            return children;
        }
    }

    abstract static class AbstractType {
    }

    static class FlatType extends AbstractType {
        private final PrimitiveType.PrimitiveTypeName primitiveTypeName;
        private final LogicalTypeAnnotation logicalTypeAnnotation;

        public FlatType(PrimitiveType.PrimitiveTypeName primitiveTypeName,
                LogicalTypeAnnotation logicalTypeAnnotation) {
            this.primitiveTypeName = primitiveTypeName;
            this.logicalTypeAnnotation = logicalTypeAnnotation;
        }

        public LogicalTypeAnnotation getLogicalTypeAnnotation() {
            return logicalTypeAnnotation;
        }

        public PrimitiveType.PrimitiveTypeName getPrimitiveTypeName() {
            return primitiveTypeName;
        }
    }

    static class ListType extends AbstractType {
        private SchemaNode child;

        public ListType() {
            child = null;
        }

        void setChild(SchemaNode child) {
            this.child = child;
        }

        boolean isEmpty() {
            return child == null;
        }

        public SchemaNode getChild() {
            return child;
        }
    }

    public static void buildParquetSchema(Types.Builder builder, SchemaNode schemaNode, String columnName)
            throws HyracksDataException {
        if (schemaNode.getType() == null) {
            throw new HyracksDataException(ErrorCode.EMPTY_TYPE_INFERRED);
        }
        AbstractType typeClass = schemaNode.getType();
        if (typeClass instanceof RecordType) {
            buildRecord(builder, (RecordType) schemaNode.getType(), columnName);
        } else if (typeClass instanceof ListType) {
            buildList(builder, (ListType) schemaNode.getType(), columnName);
        } else if (typeClass instanceof FlatType) {
            buildFlat(builder, (FlatType) schemaNode.getType(), columnName);
        }
    }

    private static void buildRecord(Types.Builder builder, RecordType type, String columnName)
            throws HyracksDataException {
        Types.BaseGroupBuilder<?, ?> childBuilder = getGroupChild(builder);
        for (Map.Entry<String, SchemaNode> entry : type.getChildren().entrySet()) {
            buildParquetSchema(childBuilder, entry.getValue(), entry.getKey());
        }
        childBuilder.named(columnName);
    }

    private static void buildList(Types.Builder builder, ListType type, String columnName) throws HyracksDataException {
        Types.BaseListBuilder<?, ?> childBuilder = getListChild(builder);
        SchemaNode child = type.child;
        if (child == null) {
            throw new HyracksDataException(ErrorCode.EMPTY_TYPE_INFERRED);
        }
        buildParquetSchema(childBuilder, child, columnName);
    }

    private static void buildFlat(Types.Builder builder, FlatType type, String columnName) throws HyracksDataException {
        if (type.getPrimitiveTypeName() == null) {
            // Not sure if this is the right thing to do here
            throw new HyracksDataException(ErrorCode.EMPTY_TYPE_INFERRED);
        }
        getPrimitiveChild(builder, type.getPrimitiveTypeName(), type.getLogicalTypeAnnotation()).named(columnName);
    }

}
