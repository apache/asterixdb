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

import org.apache.asterix.external.indexing.ExternalFileIndexAccessor;
import org.apache.asterix.external.indexing.RecordId;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SequenceLookupReader extends AbstractCharRecordLookupReader {

    public SequenceLookupReader(ExternalFileIndexAccessor snapshotAccessor, FileSystem fs, Configuration conf) {
        super(snapshotAccessor, fs, conf);
    }

    private static final Logger LOGGER = LogManager.getLogger();
    private Reader reader;
    private Writable key;

    @Override
    protected void readRecord(RecordId rid) throws IOException {
        reader.seek(rid.getOffset());
        reader.next(key, value);
    }

    @Override
    protected void closeFile() {
        if (reader == null) {
            return;
        }
        try {
            reader.close();
        } catch (Exception e) {
            LOGGER.warn("Error closing HDFS file ", e);
        }
    }

    @SuppressWarnings("deprecation")
    @Override
    protected void openFile() throws IllegalArgumentException, IOException {
        reader = new SequenceFile.Reader(fs, new Path(file.getFileName()), conf);
        key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
        value = (Text) ReflectionUtils.newInstance(reader.getValueClass(), conf);
    }

}
