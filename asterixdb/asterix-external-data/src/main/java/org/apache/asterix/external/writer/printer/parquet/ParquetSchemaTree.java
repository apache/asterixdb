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

import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.util.LogRedactionUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;

public class ParquetSchemaTree {
    private static final Logger LOGGER = LogManager.getLogger();

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

        @Override
        public String toString() {
            return type == null ? "NULL" : type.toString();
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

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("{ ");
            for (Map.Entry<String, SchemaNode> entry : children.entrySet()) {
                sb.append(entry.getKey()).append(" : ").append(entry.getValue()).append(", ");
            }
            sb.append(" }");
            return sb.toString();
        }
    }

    abstract static class AbstractType {
    }

    static class FlatType extends AbstractType {
        private ATypeTag typeTag;
        private boolean isHierarchical;

        public FlatType(ATypeTag typeTag) {
            this.typeTag = typeTag;
            isHierarchical = AsterixParquetTypeMap.HIERARCHIAL_TYPES.containsKey(typeTag);
        }

        @Override
        public String toString() {
            return typeTag.toString();
        }

        public boolean isCompatibleWith(ATypeTag typeTag) {
            if (isHierarchical) {
                return AsterixParquetTypeMap.HIERARCHIAL_TYPES.containsKey(typeTag);
            } else {
                return this.typeTag == typeTag;
            }
        }

        public void coalesce(ATypeTag typeTag) {
            if (!isCompatibleWith(typeTag) || !isHierarchical) {
                return;
            }
            this.typeTag = AsterixParquetTypeMap.maxHierarchicalType(this.typeTag, typeTag);
        }

        public LogicalTypeAnnotation getLogicalTypeAnnotation() {
            return AsterixParquetTypeMap.LOGICAL_TYPE_ANNOTATION_MAP.get(typeTag);
        }

        public PrimitiveType.PrimitiveTypeName getPrimitiveTypeName() {
            return AsterixParquetTypeMap.PRIMITIVE_TYPE_NAME_MAP.get(typeTag);
        }
    }

    static class ListType extends AbstractType {
        private SchemaNode child;

        public ListType() {
            child = null;
        }

        @Override
        public String toString() {
            return "[ " + child + " ]";
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
            LOGGER.info(
                    "Child type not set for record value with column name: " + LogRedactionUtil.userData(columnName));
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
            LOGGER.info("Child type not set for list with column name: " + LogRedactionUtil.userData(columnName));
            throw new HyracksDataException(ErrorCode.EMPTY_TYPE_INFERRED);
        }
        buildParquetSchema(childBuilder, child, columnName);
    }

    private static void buildFlat(Types.Builder builder, FlatType type, String columnName) throws HyracksDataException {
        if (type.getPrimitiveTypeName() == null) {
            LOGGER.info("Child primitive type not set for flat value with column name: "
                    + LogRedactionUtil.userData(columnName));
            // Not sure if this is the right thing to do here
            throw new HyracksDataException(ErrorCode.EMPTY_TYPE_INFERRED);
        }
        getPrimitiveChild(builder, type.getPrimitiveTypeName(), type.getLogicalTypeAnnotation()).named(columnName);
    }

}
