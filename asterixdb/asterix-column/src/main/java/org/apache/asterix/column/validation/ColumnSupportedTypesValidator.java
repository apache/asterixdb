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
package org.apache.asterix.column.validation;

import static org.apache.asterix.column.util.ColumnValuesUtil.getNormalizedTypeTag;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.asterix.column.values.reader.ColumnValueReaderFactory;
import org.apache.asterix.column.values.writer.ColumnValuesWriterFactory;
import org.apache.asterix.common.config.DatasetConfig;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.IATypeVisitor;
import org.apache.hyracks.api.exceptions.SourceLocation;

/**
 * Validates supported types for datasets with {@link DatasetConfig.DatasetFormat#COLUMN} format
 *
 * @see ColumnValuesWriterFactory
 * @see ColumnValueReaderFactory
 */
public class ColumnSupportedTypesValidator implements IATypeVisitor<Void, Set<ATypeTag>> {
    private static final Set<ATypeTag> SUPPORTED_PRIMITIVE_TYPES =
            Set.of(ATypeTag.BOOLEAN, ATypeTag.BIGINT, ATypeTag.FLOAT, ATypeTag.DOUBLE, ATypeTag.STRING, ATypeTag.UUID);
    private static final String SUPPORTED_TYPES_STRING =
            SUPPORTED_PRIMITIVE_TYPES.stream().sorted().collect(Collectors.toList()).toString();
    private static final ColumnSupportedTypesValidator VALIDATOR = new ColumnSupportedTypesValidator();

    private ColumnSupportedTypesValidator() {
    }

    /**
     * Ensure dataset format nested type includes only supported types
     *
     * @param format dataset format
     * @param type   to check
     * @throws CompilationException if an unsupported type is encountered
     */
    public static void validate(DatasetConfig.DatasetFormat format, IAType type) throws CompilationException {
        validate(format, type, null);
    }

    /**
     * Ensure dataset format nested type includes only supported types
     *
     * @param format         dataset format
     * @param type           to check
     * @param sourceLocation source location (if any)
     * @throws CompilationException if an unsupported type is encountered
     */
    public static void validate(DatasetConfig.DatasetFormat format, IAType type, SourceLocation sourceLocation)
            throws CompilationException {
        if (format != DatasetConfig.DatasetFormat.COLUMN) {
            return;
        }

        Set<ATypeTag> unsupportedTypes = new HashSet<>();
        type.accept(VALIDATOR, unsupportedTypes);
        if (!unsupportedTypes.isEmpty()) {
            String unsupportedList = unsupportedTypes.stream().sorted().collect(Collectors.toList()).toString();
            throw CompilationException.create(ErrorCode.UNSUPPORTED_COLUMN_TYPE, sourceLocation, unsupportedList,
                    SUPPORTED_TYPES_STRING);
        }
    }

    @Override
    public Void visit(ARecordType recordType, Set<ATypeTag> arg) {
        for (IAType fieldType : recordType.getFieldTypes()) {
            fieldType.accept(this, arg);
        }

        return null;
    }

    @Override
    public Void visit(AbstractCollectionType collectionType, Set<ATypeTag> arg) {
        return collectionType.getItemType().accept(this, arg);
    }

    @Override
    public Void visit(AUnionType unionType, Set<ATypeTag> arg) {
        for (IAType fieldType : unionType.getUnionList()) {
            fieldType.accept(this, arg);
        }

        return null;
    }

    @Override
    public Void visitFlat(IAType flatType, Set<ATypeTag> arg) {
        ATypeTag typeTag = getNormalizedTypeTag(flatType.getTypeTag());
        // Allow ANY
        if (typeTag != ATypeTag.ANY && typeTag != ATypeTag.NULL && typeTag != ATypeTag.MISSING
                && !SUPPORTED_PRIMITIVE_TYPES.contains(typeTag)) {
            arg.add(typeTag);
        }

        return null;
    }
}
