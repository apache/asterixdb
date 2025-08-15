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
package org.apache.asterix.external.input.record.converter;

import java.io.IOException;
import java.util.function.LongSupplier;

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.external.api.IExternalDataRuntimeContext;
import org.apache.asterix.external.api.IRawRecord;
import org.apache.asterix.external.input.record.CharArrayRecord;
import org.apache.asterix.external.input.record.RecordWithMetadataAndPK;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.dataflow.std.file.FieldCursorForDelimitedDataParser;

public class CSVToRecordWithMetadataAndPKConverter
        implements IRecordToRecordWithMetadataAndPKConverter<char[], char[]> {

    private final FieldCursorForDelimitedDataParser cursor;
    private final int valueIndex;
    private final RecordWithMetadataAndPK<char[]> recordWithMetadata;
    private final CharArrayRecord record;
    private final LongSupplier lineNumber;

    public CSVToRecordWithMetadataAndPKConverter(final int valueIndex, final char delimiter, final ARecordType metaType,
            final ARecordType recordType, final int[] keyIndicator, final int[] keyIndexes, final IAType[] keyTypes,
            IExternalDataRuntimeContext context) {
        IWarningCollector warningCollector = context.getTaskContext().getWarningCollector();
        this.cursor = new FieldCursorForDelimitedDataParser(null, delimiter, ExternalDataConstants.QUOTE,
                ExternalDataConstants.QUOTE, warningCollector, ExternalDataConstants.EMPTY_STRING, true);
        this.record = new CharArrayRecord();
        this.valueIndex = valueIndex;
        this.recordWithMetadata = new RecordWithMetadataAndPK<>(record, metaType.getFieldTypes(), recordType,
                keyIndicator, keyIndexes, keyTypes);
        lineNumber = context.getLineNumberSupplier();
    }

    @Override
    public RecordWithMetadataAndPK<char[]> convert(final IRawRecord<? extends char[]> input) throws IOException {
        record.reset();
        recordWithMetadata.reset();
        cursor.nextRecord(input.get(), input.size(), lineNumber.getAsLong());
        int i = 0;
        int j = 0;
        FieldCursorForDelimitedDataParser.Result lastResult;
        while ((lastResult = cursor.nextField()) == FieldCursorForDelimitedDataParser.Result.OK) {
            if (cursor.fieldHasEscapedQuote()) {
                cursor.eliminateEscapeChar();
            }
            if (i == valueIndex) {
                record.setValue(cursor.getBuffer(), cursor.getFieldStart(), cursor.getFieldLength());
                record.endRecord();
            } else {
                recordWithMetadata.setRawMetadata(j, cursor.getBuffer(), cursor.getFieldStart(),
                        cursor.getFieldLength());
                j++;
            }
            i++;
        }
        if (lastResult == FieldCursorForDelimitedDataParser.Result.ERROR) {
            throw new RuntimeDataException(ErrorCode.FAILED_TO_PARSE_RECORD);
        }
        return recordWithMetadata;
    }
}
