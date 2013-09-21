/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.external.dataset.adapter;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextInputFormat;

import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.hyracks.algebricks.common.exceptions.NotImplementedException;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;

/**
 * Provides functionality for fetching external data stored in an HDFS instance.
 */
@SuppressWarnings({ "deprecation", "rawtypes" })
public class HDFSAdapter extends FileSystemBasedAdapter {

    private static final long serialVersionUID = 1L;

    private transient String[] readSchedule;
    private transient boolean executed[];
    private transient InputSplit[] inputSplits;
    private transient JobConf conf;
    private transient AlgebricksPartitionConstraint clusterLocations;

    private transient String nodeName;

    public HDFSAdapter(IAType atype, String[] readSchedule, boolean[] executed, InputSplit[] inputSplits, JobConf conf,
            AlgebricksPartitionConstraint clusterLocations) {
        super(atype);
        this.readSchedule = readSchedule;
        this.executed = executed;
        this.inputSplits = inputSplits;
        this.conf = conf;
        this.clusterLocations = clusterLocations;
    }

    @Override
    public void configure(Map<String, Object> arguments) throws Exception {
        this.configuration = arguments;
        configureFormat();
    }

    public AdapterType getAdapterType() {
        return AdapterType.READ_WRITE;
    }

    @Override
    public void initialize(IHyracksTaskContext ctx) throws Exception {
        this.ctx = ctx;
        this.nodeName = ctx.getJobletContext().getApplicationContext().getNodeId();
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
        };

        return reporter;
    }

    @Override
    public InputStream getInputStream(int partition) throws IOException {

        return new InputStream() {

            private RecordReader<Object, Text> reader;
            private Object key;
            private Text value;
            private boolean hasMore = false;
            private int EOL = "\n".getBytes()[0];
            private Text pendingValue = null;
            private int currentSplitIndex = 0;

            @SuppressWarnings("unchecked")
            private boolean moveToNext() throws IOException {
                for (; currentSplitIndex < inputSplits.length; currentSplitIndex++) {
                    /**
                     * read all the partitions scheduled to the current node
                     */
                    if (readSchedule[currentSplitIndex].equals(nodeName)) {
                        /**
                         * pick an unread split to read
                         * synchronize among simultaneous partitions in the same machine
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
                        //nothing to read
                        return -1;
                    }
                }

                int numBytes = 0;
                if (pendingValue != null) {
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
                                //move to the next non-empty split
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

            private RecordReader getRecordReader(int slitIndex) throws IOException {
                if (conf.getInputFormat() instanceof SequenceFileInputFormat) {
                    SequenceFileInputFormat format = (SequenceFileInputFormat) conf.getInputFormat();
                    RecordReader reader = format.getRecordReader(
                            (org.apache.hadoop.mapred.FileSplit) inputSplits[slitIndex], conf, getReporter());
                    return reader;
                } else {
                    TextInputFormat format = (TextInputFormat) conf.getInputFormat();
                    RecordReader reader = format.getRecordReader(
                            (org.apache.hadoop.mapred.FileSplit) inputSplits[slitIndex], conf, getReporter());
                    return reader;
                }
            }

        };

    }

    @Override
    public AlgebricksPartitionConstraint getPartitionConstraint() throws Exception {
        return clusterLocations;
    }

}