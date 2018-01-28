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
package org.apache.asterix.external.input.record.reader;

import java.io.IOException;

import org.apache.asterix.external.api.IRawRecord;
import org.apache.asterix.external.api.IRecordReader;
import org.apache.asterix.external.dataflow.AbstractFeedDataFlowController;
import org.apache.asterix.external.input.record.CharArrayRecord;
import org.apache.asterix.external.input.record.GenericRecord;
import org.apache.asterix.external.input.record.RecordWithPK;
import org.apache.asterix.external.util.FeedLogManager;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

public class TestAsterixMembersReader implements IRecordReader<RecordWithPK<char[]>> {

    private final CharArrayRecord rawRecord;
    private final GenericRecord<RecordWithPK<char[]>> record;
    private final ArrayBackedValueStorage[] pkFieldValueBuffers;
    private int counter = 0;
    private final int numOfRecords = 10;
    private final StringBuilder builder = new StringBuilder();
    private static final String[] names =
            { "Abdullah", "Michael", "Till", "Yingyi", "Ildar", "Taewoo", "Young-Seok", "Murtadha", "Ian", "Steven" };

    public TestAsterixMembersReader() {
        rawRecord = new CharArrayRecord();
        pkFieldValueBuffers = new ArrayBackedValueStorage[1];
        pkFieldValueBuffers[0] = new ArrayBackedValueStorage();
        record = new GenericRecord<RecordWithPK<char[]>>(new RecordWithPK<char[]>(rawRecord, pkFieldValueBuffers));
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public boolean hasNext() throws Exception {
        return counter < numOfRecords;
    }

    @Override
    public IRawRecord<RecordWithPK<char[]>> next() throws IOException, InterruptedException {
        if (counter < numOfRecords) {
            record.get().reset();
            builder.setLength(0);
            builder.append("{\"id\":" + counter + ",\"name\":\"" + names[counter % names.length] + "\"}");
            rawRecord.set(builder);
            rawRecord.endRecord();
            pkFieldValueBuffers[0].getDataOutput().writeByte(ATypeTag.SERIALIZED_INT64_TYPE_TAG);
            pkFieldValueBuffers[0].getDataOutput().writeLong(counter);
            counter++;
            return record;
        }
        return null;
    }

    @Override
    public boolean stop() {
        return false;
    }

    @Override
    public void setController(final AbstractFeedDataFlowController controller) {
    }

    @Override
    public void setFeedLogManager(final FeedLogManager feedLogManager) {
    }

    @Override
    public boolean handleException(Throwable th) {
        return false;
    }
}
