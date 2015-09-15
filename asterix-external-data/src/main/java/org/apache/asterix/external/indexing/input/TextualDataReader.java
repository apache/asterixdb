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
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextInputFormat;

import org.apache.asterix.metadata.entities.ExternalFile;
import org.apache.hyracks.algebricks.common.exceptions.NotImplementedException;

// Used in two cases:
// 1. building an index over a dataset
// 2. performing full scan over a dataset that has built index (to provide consistent view)

@SuppressWarnings({ "rawtypes", "deprecation" })
public class TextualDataReader extends AbstractHDFSReader {

    private RecordReader<Object, Text> reader;
    private Object key;
    private Text value;
    private boolean hasMore = false;
    private int EOL = "\n".getBytes()[0];
    private Text pendingValue = null;
    private int currentSplitIndex = 0;
    private String fileName;
    private long recordOffset;
    private boolean executed[];
    private InputSplit[] inputSplits;
    private String[] readSchedule;
    private String nodeName;
    private JobConf conf;
    private List<ExternalFile> files;
    private FileSystem hadoopFS;

    public TextualDataReader(InputSplit[] inputSplits, String[] readSchedule, String nodeName, JobConf conf,
            boolean executed[], List<ExternalFile> files) throws IOException {
        this.executed = executed;
        this.inputSplits = inputSplits;
        this.readSchedule = readSchedule;
        this.nodeName = nodeName;
        this.conf = conf;
        this.files = files;
        hadoopFS = FileSystem.get(conf);
    }

    @Override
    public boolean initialize() throws Exception {
        return moveToNext();
    }

    @Override
    public Object readNext() throws Exception {
        if (reader == null) {
            return null;
        }
        recordOffset = reader.getPos();
        if (reader.next(key, value)) {
            return value;
        }
        while (moveToNext()) {
            recordOffset = reader.getPos();
            if (reader.next(key, value)) {
                return value;
            }
        }
        return null;
    }

    @Override
    public int getFileNumber() throws Exception {
        return files.get(currentSplitIndex).getFileNumber();
    }

    @Override
    public String getFileName() throws Exception {
        return files.get(currentSplitIndex).getFileName();
    }

    @Override
    public long getReaderPosition() throws Exception {
        return recordOffset;
    }

    @Override
    public int read() throws IOException {
        throw new NotImplementedException("Use read(byte[], int, int");
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
                try {
                    if (files != null) {
                        fileName = ((FileSplit) (inputSplits[currentSplitIndex])).getPath().toUri().getPath();
                        FileStatus fileStatus = hadoopFS.getFileStatus(new Path(fileName));
                        // Skip if not the same file stored in the files snapshot
                        if (fileStatus.getModificationTime() != files.get(currentSplitIndex).getLastModefiedTime()
                                .getTime())
                            continue;
                    }
                    // It is the same file
                    reader = getRecordReader(currentSplitIndex);
                } catch (Exception e) {
                    // ignore exceptions <-- This might change later -->
                    continue;
                }
                key = reader.createKey();
                value = (Text) reader.createValue();
                return true;
            }
        }
        return false;
    }

    
    private RecordReader getRecordReader(int splitIndex) throws IOException {
        RecordReader reader;
        if (conf.getInputFormat() instanceof SequenceFileInputFormat) {
            SequenceFileInputFormat format = (SequenceFileInputFormat) conf.getInputFormat();
            reader = format.getRecordReader((org.apache.hadoop.mapred.FileSplit) inputSplits[splitIndex], conf,
                    getReporter());
        } else {
            TextInputFormat format = (TextInputFormat) conf.getInputFormat();
            reader = format.getRecordReader((org.apache.hadoop.mapred.FileSplit) inputSplits[splitIndex], conf,
                    getReporter());
        }
        return reader;
    }

    // Return one record at a time <to preserve the indexing information>
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
            return numBytes;
        }
        if (numBytes < len) {
            //store the byte location
            recordOffset = reader.getPos();
            hasMore = reader.next(key, value);
            if (!hasMore) {
                while (moveToNext()) {
                    //store the byte location
                    recordOffset = reader.getPos();
                    hasMore = reader.next(key, value);
                    if (hasMore) {
                        //return the value read
                        int sizeOfNextTuple = value.getLength() + 1;
                        if (numBytes + sizeOfNextTuple > len) {
                            pendingValue = value;
                            return 0;
                        } else {
                            System.arraycopy(value.getBytes(), 0, buffer, offset + numBytes, value.getLength());
                            buffer[offset + numBytes + value.getLength()] = (byte) EOL;
                            numBytes += sizeOfNextTuple;
                            return numBytes;
                        }
                    }
                }
                return -1;
            } else {
                //return the value read
                int sizeOfNextTuple = value.getLength() + 1;
                if (numBytes + sizeOfNextTuple > len) {
                    pendingValue = value;
                    return 0;
                } else {
                    System.arraycopy(value.getBytes(), 0, buffer, offset + numBytes, value.getLength());
                    buffer[offset + numBytes + value.getLength()] = (byte) EOL;
                    numBytes += sizeOfNextTuple;
                    return numBytes;
                }
            }
        }
        return numBytes;
    }
}
