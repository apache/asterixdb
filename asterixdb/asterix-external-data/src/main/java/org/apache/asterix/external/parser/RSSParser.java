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
package org.apache.asterix.external.parser;

import java.io.DataOutput;

import org.apache.asterix.builders.RecordBuilder;
import org.apache.asterix.external.api.IDataParser;
import org.apache.asterix.external.api.IRawRecord;
import org.apache.asterix.external.api.IRecordDataParser;
import org.apache.asterix.om.base.AMutableRecord;
import org.apache.asterix.om.base.AMutableString;
import org.apache.asterix.om.types.ARecordType;
import org.apache.hyracks.api.exceptions.HyracksDataException;

import com.rometools.rome.feed.synd.SyndEntry;

public class RSSParser implements IRecordDataParser<SyndEntry> {
    private long id = 0;
    private AMutableString[] mutableFields;
    private String[] tupleFieldValues;
    private AMutableRecord mutableRecord;
    private RecordBuilder recordBuilder = new RecordBuilder();
    private int numFields;

    public RSSParser(ARecordType recordType) {
        mutableFields = new AMutableString[] { new AMutableString(null), new AMutableString(null),
                new AMutableString(null), new AMutableString(null) };
        mutableRecord = new AMutableRecord(recordType, mutableFields);
        tupleFieldValues = new String[recordType.getFieldNames().length];
        numFields = recordType.getFieldNames().length;
    }

    @Override
    public void parse(IRawRecord<? extends SyndEntry> record, DataOutput out) throws HyracksDataException {
        SyndEntry entry = record.get();
        tupleFieldValues[0] = String.valueOf(id);
        tupleFieldValues[1] = entry.getTitle();
        tupleFieldValues[2] = entry.getDescription().getValue();
        tupleFieldValues[3] = entry.getLink();
        for (int i = 0; i < numFields; i++) {
            mutableFields[i].setValue(tupleFieldValues[i]);
            mutableRecord.setValueAtPos(i, mutableFields[i]);
        }
        recordBuilder.reset(mutableRecord.getType());
        recordBuilder.init();
        IDataParser.writeRecord(mutableRecord, out, recordBuilder);
        id++;
    }
}
