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

package org.apache.asterix.om.pointables.printer.csv;

import static org.apache.asterix.common.exceptions.ErrorCode.COPY_TO_SCHEMA_MISMATCH;
import static org.apache.asterix.om.types.hierachy.ATypeHierarchy.isCompatible;

import java.io.PrintStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.asterix.om.pointables.ARecordVisitablePointable;
import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.pointables.printer.ARecordPrinter;
import org.apache.asterix.om.pointables.printer.IPrintVisitor;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.api.exceptions.Warning;
import org.apache.hyracks.util.string.UTF8StringUtil;

public class ACSVRecordPrinter extends ARecordPrinter {
    private static final List<ATypeTag> supportedTypes = List.of(ATypeTag.TINYINT, ATypeTag.SMALLINT, ATypeTag.INTEGER,
            ATypeTag.BIGINT, ATypeTag.UINT8, ATypeTag.UINT16, ATypeTag.UINT64, ATypeTag.FLOAT, ATypeTag.DOUBLE,
            ATypeTag.STRING, ATypeTag.BOOLEAN, ATypeTag.DATETIME, ATypeTag.UINT32, ATypeTag.DATE, ATypeTag.TIME);

    private final IWarningCollector warningCollector;
    private final ARecordType schema;
    private final boolean header;
    private final String recordDelimiter;
    private final Map<String, ATypeTag> recordSchemaDetails = new HashMap<>();
    private List<String> expectedFieldNames;
    private List<IAType> expectedFieldTypes;

    public ACSVRecordPrinter(IWarningCollector warningCollector, boolean header, String fieldSeparator,
            String recordDelimiter, ARecordType schema) {
        super("", "", fieldSeparator, null);
        this.warningCollector = warningCollector;
        this.header = header;
        this.schema = schema;
        this.recordDelimiter = recordDelimiter;
        if (schema != null) {
            this.expectedFieldNames = Arrays.asList(schema.getFieldNames());
            this.expectedFieldTypes = Arrays.asList(schema.getFieldTypes());
        }
    }

    @Override
    public void printRecord(ARecordVisitablePointable recordAccessor, PrintStream ps, IPrintVisitor visitor,
            boolean firstRecord) throws HyracksDataException {
        // backward compatibility - no schema provided, print it as is from recordAccessor
        if (schema == null) {
            super.printRecord(recordAccessor, ps, visitor);
        } else {
            printSchemaFullRecord(recordAccessor, ps, visitor, firstRecord);
        }
    }

    private void printSchemaFullRecord(ARecordVisitablePointable recordAccessor, PrintStream ps, IPrintVisitor visitor,
            boolean firstRecord) throws HyracksDataException {
        // check the schema for the record
        // try producing the record into the record of expected schema
        if (isValidSchema(recordAccessor)) {
            nameVisitorArg.first = ps;
            itemVisitorArg.first = ps;

            if (!firstRecord) {
                ps.print(recordDelimiter);
            } else if (header) {
                // it's first record and header is true
                printHeader(recordAccessor, ps, visitor);
                ps.print(recordDelimiter);
            }

            final List<IVisitablePointable> fieldNames = recordAccessor.getFieldNames();
            final List<IVisitablePointable> fieldValues = recordAccessor.getFieldValues();

            boolean first = true;
            for (int i = 0; i < fieldNames.size(); ++i) {
                final IVisitablePointable fieldNamePointable = fieldNames.get(i);
                String fieldName = UTF8StringUtil.toString(fieldNamePointable.getByteArray(),
                        fieldNamePointable.getStartOffset() + 1);
                final IVisitablePointable fieldValue = fieldValues.get(i);
                ATypeTag expectedTypeTag = recordSchemaDetails.get(fieldName);
                if (first) {
                    first = false;
                } else {
                    ps.print(fieldSeparator);
                }
                printField(ps, visitor, fieldNamePointable, fieldValue, expectedTypeTag);
            }
        }
    }

    private boolean isValidSchema(ARecordVisitablePointable recordAccessor) {
        recordSchemaDetails.clear();
        final List<IVisitablePointable> actualFieldNamePointables = recordAccessor.getFieldNames();
        final List<IVisitablePointable> actualFieldValuePointables = recordAccessor.getFieldValues();
        if (actualFieldNamePointables.size() != expectedFieldNames.size()) {
            warnMismatchType("expected schema has '" + expectedFieldNames.size() + "' fields but actual record has '"
                    + actualFieldNamePointables.size() + "' fields");
            return false;
        }

        for (int i = 0; i < actualFieldNamePointables.size(); i++) {
            String actualFieldName = getFieldName(actualFieldNamePointables.get(i));
            ATypeTag actualValueType = getValueType(actualFieldValuePointables.get(i));
            if (!expectedFieldNames.contains(actualFieldName)) {
                warnMismatchType("field '" + actualFieldName + "' does not exist in the expected schema");
                return false;
            }

            boolean isNullable = false;
            IAType expectedIAType = expectedFieldTypes.get(expectedFieldNames.indexOf(actualFieldName));
            ATypeTag expectedType = expectedIAType.getTypeTag();
            if (expectedType.equals(ATypeTag.UNION)) {
                AUnionType unionType = (AUnionType) expectedIAType;
                expectedType = unionType.getActualType().getTypeTag();
                isNullable = unionType.isNullableType();
            }

            if (actualValueType.equals(ATypeTag.MISSING)) {
                warnMismatchType("field '" + actualFieldName + "' cannot be missing");
                return false;
            }

            if ((actualValueType.equals(ATypeTag.NULL) && !isNullable)) {
                warnMismatchType("field '" + actualFieldName + "' is required, found 'null'");
                return false;
            }

            if (actualValueType.equals(ATypeTag.NULL)) {
                recordSchemaDetails.put(actualFieldName, ATypeTag.NULL);
                continue;
            }

            if (!supportedTypes.contains(actualValueType)) {
                warnMismatchType("type '" + actualValueType + "' for field '" + actualFieldName
                        + "' is not supported in CSV format");
                return false;
            }

            if (!isCompatible(actualValueType, expectedType)) {
                warnMismatchType("incompatible type for field '" + actualFieldName + "', expected '" + expectedType
                        + "' but got '" + actualValueType + "'");
                return false;
            }
            recordSchemaDetails.put(actualFieldName, expectedType);
        }
        return true;
    }

    private void printHeader(ARecordVisitablePointable recordAccessor, PrintStream ps, IPrintVisitor visitor)
            throws HyracksDataException {
        boolean first = true;
        for (IVisitablePointable fieldName : recordAccessor.getFieldNames()) {
            if (first) {
                first = false;
            } else {
                ps.print(fieldSeparator);
            }
            printFieldName(ps, visitor, fieldName);
        }
    }

    private String getFieldName(IVisitablePointable pointable) {
        return UTF8StringUtil.toString(pointable.getByteArray(), pointable.getStartOffset() + 1);
    }

    private ATypeTag getValueType(IVisitablePointable pointable) {
        return EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(pointable.getByteArray()[pointable.getStartOffset()]);
    }

    private void warnMismatchType(String warningMessage) {
        if (warningCollector.shouldWarn()) {
            warningCollector.warn(Warning.of(null, COPY_TO_SCHEMA_MISMATCH, warningMessage));
        }
    }
}
