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
package org.apache.asterix.external.indexing.input;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.Counters.Counter;

import org.apache.hyracks.algebricks.common.exceptions.NotImplementedException;

public class TextualFullScanDataReader extends InputStream {

    private RecordReader<Object, Text> reader;
    private Object key;
    private Text value;
    private boolean hasMore = false;
    private int EOL = "\n".getBytes()[0];
    private Text pendingValue = null;
    private int currentSplitIndex = 0;
    private boolean executed[];
    private InputSplit[] inputSplits;
    private String[] readSchedule;
    private String nodeName;
    private JobConf conf;

    public TextualFullScanDataReader(boolean executed[], InputSplit[] inputSplits, String[] readSchedule,
            String nodeName, JobConf conf) {
        this.executed = executed;
        this.inputSplits = inputSplits;
        this.readSchedule = readSchedule;
        this.nodeName = nodeName;
        this.conf = conf;
    }

    @Override
    public int available() {
        return 1;
    }

    @SuppressWarnings("unchecked")
    private boolean moveToNext() throws IOException {
        for (; currentSplitIndex < inputSplits.length; currentSplitIndex++) {
            /**
             * read all the partitions scheduled to the current node
             */
            if (readSchedule[currentSplitIndex].equals(nodeName)) {
                /**
                 * pick an unread split to read synchronize among
                 * simultaneous partitions in the same machine
                 */
                synchronized (executed) {
                    if (executed[currentSplitIndex] == false) {
                        executed[currentSplitIndex] = true;
                    } else {
                        continue;
                    }
                }

                /**
                 * read the split
                 */
                reader = getRecordReader(currentSplitIndex);
                key = reader.createKey();
                value = (Text) reader.createValue();
                return true;
            }
        }
        return false;
    }

    @Override
    public int read(byte[] buffer, int offset, int len) throws IOException {
        if (reader == null) {
            if (!moveToNext()) {
                // nothing to read
                return -1;
            }
        }

        int numBytes = 0;
        if (pendingValue != null) {
            int sizeOfNextTuple = pendingValue.getLength() + 1;
            if (sizeOfNextTuple > len) {
                return 0;
            }
            System.arraycopy(pendingValue.getBytes(), 0, buffer, offset + numBytes, pendingValue.getLength());
            buffer[offset + numBytes + pendingValue.getLength()] = (byte) EOL;
            numBytes += pendingValue.getLength() + 1;
            pendingValue = null;
        }

        while (numBytes < len) {
            hasMore = reader.next(key, value);
            if (!hasMore) {
                while (moveToNext()) {
                    hasMore = reader.next(key, value);
                    if (hasMore) {
                        // move to the next non-empty split
                        break;
                    }
                }
            }
            if (!hasMore) {
                return (numBytes == 0) ? -1 : numBytes;
            }
            int sizeOfNextTuple = value.getLength() + 1;
            if (numBytes + sizeOfNextTuple > len) {
                // cannot add tuple to current buffer
                // but the reader has moved pass the fetched tuple
                // we need to store this for a subsequent read call.
                // and return this then.
                pendingValue = value;
                break;
            } else {
                System.arraycopy(value.getBytes(), 0, buffer, offset + numBytes, value.getLength());
                buffer[offset + numBytes + value.getLength()] = (byte) EOL;
                numBytes += sizeOfNextTuple;
            }
        }
        return numBytes;
    }

    @Override
    public int read() throws IOException {
        throw new NotImplementedException("Use read(byte[], int, int");
    }

    @SuppressWarnings("rawtypes")
    private RecordReader getRecordReader(int splitIndex) throws IOException {
        if (conf.getInputFormat() instanceof SequenceFileInputFormat) {
            SequenceFileInputFormat format = (SequenceFileInputFormat) conf.getInputFormat();
            RecordReader reader = format.getRecordReader((org.apache.hadoop.mapred.FileSplit) inputSplits[splitIndex],
                    conf, getReporter());
            return reader;
        } else {
            TextInputFormat format = (TextInputFormat) conf.getInputFormat();
            RecordReader reader = format.getRecordReader((org.apache.hadoop.mapred.FileSplit) inputSplits[splitIndex],
                    conf, getReporter());
            return reader;
        }
    }

    private Reporter getReporter() {
        Reporter reporter = new Reporter() {

            @Override
            public Counter getCounter(Enum<?> arg0) {
                return null;
            }

            @Override
            public Counter getCounter(String arg0, String arg1) {
                return null;
            }

            @Override
            public InputSplit getInputSplit() throws UnsupportedOperationException {
                return null;
            }

            @Override
            public void incrCounter(Enum<?> arg0, long arg1) {
            }

            @Override
            public void incrCounter(String arg0, String arg1, long arg2) {
            }

            @Override
            public void setStatus(String arg0) {
            }

            @Override
            public void progress() {
            }

            @Override
            public float getProgress() {
                return 0.0f;
            }
        };

        return reporter;
    }
}
