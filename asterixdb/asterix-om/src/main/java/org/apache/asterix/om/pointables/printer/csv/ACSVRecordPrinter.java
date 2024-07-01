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
import org.apache.hyracks.util.string.UTF8StringUtil;

public class ACSVRecordPrinter extends ARecordPrinter {
    private ARecordType schema;
    private boolean firstRecord;
    private boolean header;
    private final String recordDelimiter;
    private static final List<ATypeTag> supportedTypes = List.of(ATypeTag.TINYINT, ATypeTag.SMALLINT, ATypeTag.INTEGER,
            ATypeTag.BIGINT, ATypeTag.UINT8, ATypeTag.UINT16, ATypeTag.UINT64, ATypeTag.FLOAT, ATypeTag.DOUBLE,
            ATypeTag.STRING, ATypeTag.BOOLEAN, ATypeTag.DATETIME, ATypeTag.UINT32, ATypeTag.DATE, ATypeTag.TIME);

    public ACSVRecordPrinter(final String startRecord, final String endRecord, final String fieldSeparator,
            final String fieldNameSeparator, String recordDelimiter, ARecordType schema, String headerStr) {
        super(startRecord, endRecord, fieldSeparator, fieldNameSeparator);
        this.schema = schema;
        this.header = headerStr != null && Boolean.parseBoolean(headerStr);
        this.firstRecord = true;
        this.recordDelimiter = recordDelimiter;
    }

    @Override
    public void printRecord(ARecordVisitablePointable recordAccessor, PrintStream ps, IPrintVisitor visitor)
            throws HyracksDataException {
        // backward compatibility -- No Schema print it as it is from recordAccessor
        if (schema == null) {
            super.printRecord(recordAccessor, ps, visitor);
        } else {
            printSchemaFullRecord(recordAccessor, ps, visitor);
        }
    }

    private void printSchemaFullRecord(ARecordVisitablePointable recordAccessor, PrintStream ps, IPrintVisitor visitor)
            throws HyracksDataException {
        // check the schema for the record
        // try producing the record into the record of expected schema
        Map<String, ATypeTag> schemaDetails = new HashMap<>();
        if (checkCSVSchema(recordAccessor, schemaDetails)) {
            nameVisitorArg.first = ps;
            itemVisitorArg.first = ps;
            if (header) {
                addHeader(recordAccessor, ps, visitor);
            }
            // add record delimiter
            // by default the separator between the header and the records is "\n"
            if (firstRecord) {
                firstRecord = false;
            } else {
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
                final ATypeTag typeTag = EnumDeserializer.ATYPETAGDESERIALIZER
                        .deserialize(fieldValue.getByteArray()[fieldValue.getStartOffset()]);
                ATypeTag expectedTypeTag = schemaDetails.get(fieldName);
                if (!isCompatible(typeTag, expectedTypeTag)) {
                    expectedTypeTag = ATypeTag.NULL;
                }
                if (first) {
                    first = false;
                } else {
                    ps.print(fieldSeparator);
                }
                printField(ps, visitor, fieldNamePointable, fieldValue, expectedTypeTag);
            }
        }
    }

    private boolean checkCSVSchema(ARecordVisitablePointable recordAccessor, Map<String, ATypeTag> schemaDetails) {
        final List<IVisitablePointable> fieldNames = recordAccessor.getFieldNames();
        final List<IVisitablePointable> fieldValues = recordAccessor.getFieldValues();
        final List<String> expectedFieldNames = Arrays.asList(schema.getFieldNames());
        final List<IAType> expectedFieldTypes = Arrays.asList(schema.getFieldTypes());
        if (fieldNames.size() != expectedFieldNames.size()) {
            // todo: raise warning about schema mismatch
            return false;
        }
        for (int i = 0; i < fieldNames.size(); ++i) {
            final IVisitablePointable fieldName = fieldNames.get(i);
            String fieldColumnName = UTF8StringUtil.toString(fieldName.getByteArray(), fieldName.getStartOffset() + 1);
            final IVisitablePointable fieldValue = fieldValues.get(i);
            final ATypeTag typeTag = EnumDeserializer.ATYPETAGDESERIALIZER
                    .deserialize(fieldValue.getByteArray()[fieldValue.getStartOffset()]);
            ATypeTag expectedType;
            boolean canNull = false;
            if (expectedFieldNames.contains(fieldColumnName)) {
                IAType expectedIAType = expectedFieldTypes.get(expectedFieldNames.indexOf(fieldColumnName));
                if (!supportedTypes.contains(expectedIAType.getTypeTag())) {
                    if (expectedIAType.getTypeTag().equals(ATypeTag.UNION)) {
                        AUnionType unionType = (AUnionType) expectedIAType;
                        expectedType = unionType.getActualType().getTypeTag();
                        canNull = unionType.isNullableType();
                        if (!supportedTypes.contains(expectedType)) {
                            // unsupported DataType
                            return false;
                        }
                    } else {
                        // todo: unexpected type
                        return false;
                    }
                } else {
                    expectedType = expectedIAType.getTypeTag();
                }
                schemaDetails.put(fieldColumnName, expectedType);
            } else {
                // todo: raise warning about schema mismatch
                return false;
            }
            if (typeTag.equals(ATypeTag.MISSING) || (typeTag.equals(ATypeTag.NULL) && !canNull)) {
                // todo: raise warning about schema mismatch
                return false;
            }
            if (!isCompatible(typeTag, expectedType) && !canNull) {
                return false;
            }
        }
        return true;
    }

    private void addHeader(ARecordVisitablePointable recordAccessor, PrintStream ps, IPrintVisitor visitor)
            throws HyracksDataException {
        //check if it is a first record
        if (firstRecord) {
            final List<IVisitablePointable> fieldNames = recordAccessor.getFieldNames();
            boolean first = true;
            for (int i = 0; i < fieldNames.size(); ++i) {
                if (first) {
                    first = false;
                } else {
                    ps.print(fieldSeparator);
                }
                printFieldName(ps, visitor, fieldNames.get(i));
            }
            firstRecord = false;
        }
    }
}
