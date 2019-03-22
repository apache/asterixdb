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

import org.apache.asterix.external.api.IRawRecord;
import org.apache.asterix.external.indexing.ExternalFileIndexAccessor;
import org.apache.asterix.external.indexing.RecordId;
import org.apache.asterix.external.input.record.GenericRecord;
import org.apache.asterix.hivecompat.io.RCFile.Reader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RCLookupReader extends AbstractHDFSLookupRecordReader<BytesRefArrayWritable> {
    public RCLookupReader(ExternalFileIndexAccessor snapshotAccessor, FileSystem fs, Configuration conf) {
        super(snapshotAccessor, fs, conf);
    }

    private static final Logger LOGGER = LogManager.getLogger();
    private Reader reader;
    private LongWritable key = new LongWritable();
    private BytesRefArrayWritable value = new BytesRefArrayWritable();
    private GenericRecord<BytesRefArrayWritable> record = new GenericRecord<BytesRefArrayWritable>();
    private long offset;
    private int row;

    @Override
    public Class<?> getRecordClass() throws IOException {
        return Writable.class;
    }

    @Override
    protected IRawRecord<BytesRefArrayWritable> lookup(RecordId rid) throws IOException {
        if (rid.getOffset() != offset) {
            offset = rid.getOffset();
            if (reader.getPosition() != offset)
                reader.seek(offset);
            reader.resetBuffer();
            row = -1;
        }

        // skip rows to the record row
        while (row < rid.getRow()) {
            reader.next(key);
            reader.getCurrentRow(value);
            row++;
        }
        record.set(value);
        return record;
    }

    @Override
    protected void closeFile() {
        if (reader == null) {
            return;
        }
        try {
            reader.close();
        } catch (Exception e) {
            LOGGER.warn("Error closing HDFS file", e);
        }
    }

    @Override
    protected void openFile() throws IllegalArgumentException, IOException {
        reader = new Reader(fs, new Path(file.getFileName()), conf);
        offset = -1;
        row = -1;
    }
}
