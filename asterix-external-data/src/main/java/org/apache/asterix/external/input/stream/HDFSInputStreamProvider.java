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
package org.apache.asterix.external.input.stream;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.asterix.external.api.IInputStreamProvider;
import org.apache.asterix.external.indexing.ExternalFile;
import org.apache.asterix.external.input.record.reader.HDFSRecordReader;
import org.apache.asterix.external.provider.ExternalIndexerProvider;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

public class HDFSInputStreamProvider<K> extends HDFSRecordReader<K, Text> implements IInputStreamProvider {

    public HDFSInputStreamProvider(boolean read[], InputSplit[] inputSplits, String[] readSchedule, String nodeName,
            JobConf conf, Map<String, String> configuration, List<ExternalFile> snapshot) throws Exception {
        super(read, inputSplits, readSchedule, nodeName, conf);
        value = new Text();
        configure(configuration);
        if (snapshot != null) {
            setSnapshot(snapshot);
            setIndexer(ExternalIndexerProvider.getIndexer(configuration));
            if (currentSplitIndex < snapshot.size()) {
                indexer.reset(this);
            }
        }
    }

    @Override
    public AInputStream getInputStream() throws Exception {
        return new HDFSInputStream();
    }

    private class HDFSInputStream extends AInputStream {
        int pos = 0;

        @Override
        public int read() throws IOException {
            if (value.getLength() < pos) {
                if (!readMore()) {
                    return -1;
                }
            } else if (value.getLength() == pos) {
                pos++;
                return ExternalDataConstants.EOL;
            }
            return value.getBytes()[pos++];
        }

        private int readRecord(byte[] buffer, int offset, int len) {
            int actualLength = value.getLength() + 1;
            if ((actualLength - pos) > len) {
                //copy partial record
                System.arraycopy(value.getBytes(), pos, buffer, offset, len);
                pos += len;
                return len;
            } else {
                int numBytes = value.getLength() - pos;
                System.arraycopy(value.getBytes(), pos, buffer, offset, numBytes);
                buffer[offset + numBytes] = ExternalDataConstants.LF;
                pos += numBytes;
                numBytes++;
                return numBytes;
            }
        }

        @Override
        public int read(byte[] buffer, int offset, int len) throws IOException {
            if (value.getLength() > pos) {
                return readRecord(buffer, offset, len);
            }
            if (!readMore()) {
                return -1;
            }
            return readRecord(buffer, offset, len);
        }

        private boolean readMore() throws IOException {
            try {
                pos = 0;
                return HDFSInputStreamProvider.this.hasNext();
            } catch (Exception e) {
                throw new IOException(e);
            }
        }

        @Override
        public boolean skipError() throws Exception {
            return true;
        }

        @Override
        public boolean stop() throws Exception {
            return false;
        }
    }
}