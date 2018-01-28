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
package org.apache.asterix.external.input.record.reader.hdfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;

import org.apache.asterix.external.api.IRawRecord;
import org.apache.asterix.external.indexing.ExternalFileIndexAccessor;
import org.apache.asterix.external.indexing.RecordId;
import org.apache.asterix.external.input.record.CharArrayRecord;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;

public abstract class AbstractCharRecordLookupReader extends AbstractHDFSLookupRecordReader<char[]> {
    public AbstractCharRecordLookupReader(ExternalFileIndexAccessor snapshotAccessor, FileSystem fs,
            Configuration conf) {
        super(snapshotAccessor, fs, conf);
    }

    protected CharArrayRecord record = new CharArrayRecord();
    protected Text value = new Text();
    protected CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder();
    protected ByteBuffer reusableByteBuffer = ByteBuffer.allocateDirect(ExternalDataConstants.DEFAULT_BUFFER_SIZE);
    protected CharBuffer reusableCharBuffer = CharBuffer.allocate(ExternalDataConstants.DEFAULT_BUFFER_SIZE);

    @Override
    public Class<?> getRecordClass() throws IOException {
        return char[].class;
    }

    @Override
    protected IRawRecord<char[]> lookup(RecordId rid) throws IOException {
        record.reset();
        readRecord(rid);
        writeRecord();
        return record;
    }

    protected abstract void readRecord(RecordId rid) throws IOException;

    private void writeRecord() throws IOException {
        reusableByteBuffer.clear();
        if (reusableByteBuffer.remaining() < value.getLength()) {
            reusableByteBuffer = ByteBuffer
                    .allocateDirect((int) (value.getLength() * ExternalDataConstants.DEFAULT_BUFFER_INCREMENT_FACTOR));
        }
        reusableByteBuffer.put(value.getBytes(), 0, value.getLength());
        reusableByteBuffer.flip();
        while (reusableByteBuffer.hasRemaining()) {
            reusableCharBuffer.clear();
            decoder.decode(reusableByteBuffer, reusableCharBuffer, false);
            reusableCharBuffer.flip();
            record.append(reusableCharBuffer);
        }
        record.endRecord();
    }
}
