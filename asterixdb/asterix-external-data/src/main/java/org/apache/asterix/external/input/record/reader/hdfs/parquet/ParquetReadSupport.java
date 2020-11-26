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

import java.util.Collections;
import java.util.Map;

import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type.Repetition;
import org.apache.parquet.schema.Types;
import org.apache.parquet.schema.Types.GroupBuilder;
import org.apache.parquet.schema.Types.MessageTypeBuilder;

public class ParquetReadSupport extends ReadSupport<IValueReference> {
    private static final PrimitiveType NULL = Types.optional(PrimitiveTypeName.BOOLEAN).named("NULL");

    @Override
    public ReadContext init(InitContext context) {
        final String requestedSchemaString = context.getConfiguration().get(ExternalDataConstants.KEY_REQUESTED_FIELDS);
        final MessageType requestedSchema = getRequestedSchema(requestedSchemaString, context.getFileSchema());
        return new ReadContext(requestedSchema, Collections.emptyMap());
    }

    @Override
    public RecordMaterializer<IValueReference> prepareForRead(Configuration configuration,
            Map<String, String> keyValueMetaData, MessageType fileSchema, ReadContext readContext) {
        return new ADMRecordMaterializer(readContext);
    }

    private static class ADMRecordMaterializer extends RecordMaterializer<IValueReference> {
        private final RootConverter rootConverter;

        public ADMRecordMaterializer(ReadContext readContext) {
            rootConverter = new RootConverter(readContext.getRequestedSchema());
        }

        @Override
        public IValueReference getCurrentRecord() {
            return rootConverter.getRecord();
        }

        @Override
        public GroupConverter getRootConverter() {
            return rootConverter;
        }

    }

    private static MessageType getRequestedSchema(String requestedSchemaString, MessageType fileSchema) {
        if ("*".equals(requestedSchemaString)) {
            return fileSchema;
        }

        final MessageTypeBuilder builder = Types.buildMessage();
        final String[] paths = requestedSchemaString.split(",");
        for (int i = 0; i < paths.length; i++) {
            buildRequestedType(paths[i].trim().split("[.]"), builder, fileSchema, 0);
        }

        return builder.named("asterix");

    }

    private static void buildRequestedType(String[] fieldNames, GroupBuilder<?> builder, GroupType groupType,
            int start) {
        final String fieldName = fieldNames[start].trim();

        Type type = getType(groupType, fieldName);
        if (type != NULL && start < fieldNames.length - 1) {
            final GroupBuilder<GroupType> innerFieldBuilder = Types.buildGroup(Repetition.OPTIONAL);
            buildRequestedType(fieldNames, innerFieldBuilder, type.asGroupType(), start + 1);
            builder.addField(innerFieldBuilder.named(fieldName));
        } else {
            builder.addField(type);
        }
    }

    private static Type getType(GroupType groupType, String fieldName) {
        if (groupType.containsField(fieldName)) {
            return groupType.getType(fieldName);
        }
        return NULL;
    }

}
