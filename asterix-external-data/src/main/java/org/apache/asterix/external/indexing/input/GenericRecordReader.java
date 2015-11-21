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

import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hyracks.algebricks.common.exceptions.NotImplementedException;

/**
 * This class can be used by any input format to perform full scan operations
 */

@SuppressWarnings({ "rawtypes", "unchecked" })
public class GenericRecordReader extends AbstractHDFSReader {

    protected RecordReader reader;
    protected Object key;
    protected Object value;
    protected int currentSplitIndex = 0;
    protected boolean executed[];
    protected InputSplit[] inputSplits;
    protected String[] readSchedule;
    protected String nodeName;
    protected JobConf conf;

    public GenericRecordReader(InputSplit[] inputSplits, String[] readSchedule, String nodeName, JobConf conf,
            boolean executed[]) {
        this.inputSplits = inputSplits;
        this.readSchedule = readSchedule;
        this.nodeName = nodeName;
        this.conf = conf;
        this.executed = executed;
    }

    @Override
    public boolean initialize() throws IOException {
        return moveToNext();
    }

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
                value = reader.createValue();
                return true;
            }
        }
        return false;
    }

    protected RecordReader getRecordReader(int slitIndex) throws IOException {
        RecordReader reader = conf.getInputFormat().getRecordReader(inputSplits[slitIndex], conf, getReporter());
        return reader;
    }

    @Override
    public Object readNext() throws IOException {
        if (reader == null) {
            return null;
        }
        if (reader.next(key, value)) {
            return value;
        }
        while (moveToNext()) {
            if (reader.next(key, value)) {
                return value;
            }
        }
        return null;
    }

    @Override
    public String getFileName() throws Exception {
        return null;
    }

    @Override
    public long getReaderPosition() throws Exception {
        return reader.getPos();
    }

    @Override
    public int getFileNumber() throws Exception {
        throw new NotImplementedException("This reader doesn't support this function");
    }

    @Override
    public int read(byte[] buffer, int offset, int len) throws IOException {
        throw new NotImplementedException("Use readNext()");
    }

    @Override
    public int read() throws IOException {
        throw new NotImplementedException("Use readNext()");
    }

}
