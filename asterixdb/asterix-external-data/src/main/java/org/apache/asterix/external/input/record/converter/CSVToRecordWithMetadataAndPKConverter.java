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

import org.apache.asterix.external.api.IRawRecord;
import org.apache.asterix.external.input.record.CharArrayRecord;
import org.apache.asterix.external.input.record.RecordWithMetadataAndPK;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.dataflow.std.file.FieldCursorForDelimitedDataParser;

public class CSVToRecordWithMetadataAndPKConverter
        implements IRecordToRecordWithMetadataAndPKConverter<char[], char[]> {

    private final FieldCursorForDelimitedDataParser cursor;
    private final int valueIndex;
    private final RecordWithMetadataAndPK<char[]> recordWithMetadata;
    private final CharArrayRecord record;

    public CSVToRecordWithMetadataAndPKConverter(final int valueIndex, final char delimiter, final ARecordType metaType,
            final ARecordType recordType, final int[] keyIndicator, final int[] keyIndexes, final IAType[] keyTypes) {
        this.cursor = new FieldCursorForDelimitedDataParser(null, delimiter, ExternalDataConstants.QUOTE);
        this.record = new CharArrayRecord();
        this.valueIndex = valueIndex;
        this.recordWithMetadata = new RecordWithMetadataAndPK<char[]>(record, metaType.getFieldTypes(), recordType,
                keyIndicator, keyIndexes, keyTypes);
    }

    @Override
    public RecordWithMetadataAndPK<char[]> convert(final IRawRecord<? extends char[]> input) throws IOException {
        record.reset();
        recordWithMetadata.reset();
        cursor.nextRecord(input.get(), input.size());
        int i = 0;
        int j = 0;
        while (cursor.nextField()) {
            if (cursor.isDoubleQuoteIncludedInThisField) {
                cursor.eliminateDoubleQuote(cursor.buffer, cursor.fStart, cursor.fEnd - cursor.fStart);
                cursor.fEnd -= cursor.doubleQuoteCount;
                cursor.isDoubleQuoteIncludedInThisField = false;
            }
            if (i == valueIndex) {
                record.setValue(cursor.buffer, cursor.fStart, cursor.fEnd - cursor.fStart);
                record.endRecord();
            } else {
                recordWithMetadata.setRawMetadata(j, cursor.buffer, cursor.fStart, cursor.fEnd - cursor.fStart);
                j++;
            }
            i++;
        }
        return recordWithMetadata;
    }
}
