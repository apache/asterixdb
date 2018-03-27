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
import java.util.List;

import org.apache.asterix.external.api.IExternalIndexer;
import org.apache.asterix.external.api.IIndexingDatasource;
import org.apache.asterix.external.api.IRawRecord;
import org.apache.asterix.external.api.IRecordReader;
import org.apache.asterix.external.dataflow.AbstractFeedDataFlowController;
import org.apache.asterix.external.indexing.ExternalFile;
import org.apache.asterix.external.input.record.GenericRecord;
import org.apache.asterix.external.util.FeedLogManager;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class HDFSRecordReader<K, V extends Writable> implements IRecordReader<Writable>, IIndexingDatasource {

    protected RecordReader<K, Writable> reader;
    protected V value = null;
    protected K key = null;
    protected int currentSplitIndex = 0;
    protected boolean read[];
    protected InputFormat<?, ?> inputFormat;
    protected InputSplit[] inputSplits;
    protected String[] readSchedule;
    protected String nodeName;
    protected JobConf conf;
    protected GenericRecord<Writable> record;
    // Indexing variables
    protected final IExternalIndexer indexer;
    protected final List<ExternalFile> snapshot;
    protected final FileSystem hdfs;

    public HDFSRecordReader(boolean read[], InputSplit[] inputSplits, String[] readSchedule, String nodeName,
            JobConf conf, List<ExternalFile> snapshot, IExternalIndexer indexer) throws IOException {
        this.read = read;
        this.inputSplits = inputSplits;
        this.readSchedule = readSchedule;
        this.nodeName = nodeName;
        this.conf = conf;
        this.inputFormat = conf.getInputFormat();
        this.reader = new EmptyRecordReader<K, Writable>();
        this.record = new GenericRecord<Writable>();
        this.indexer = indexer;
        this.snapshot = snapshot;
        this.hdfs = FileSystem.get(conf);
        nextInputSplit();
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }

    @Override
    public boolean hasNext() throws Exception {
        if (reader.next(key, value)) {
            return true;
        }
        while (nextInputSplit()) {
            if (reader.next(key, value)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public IRawRecord<Writable> next() throws IOException {
        record.set(value);
        return record;
    }

    private boolean nextInputSplit() throws IOException {
        for (; currentSplitIndex < inputSplits.length; currentSplitIndex++) {
            /**
             * read all the partitions scheduled to the current node
             */
            if (readSchedule[currentSplitIndex].equals(nodeName)) {
                /**
                 * pick an unread split to read synchronize among
                 * simultaneous partitions in the same machine
                 */
                synchronized (read) {
                    if (read[currentSplitIndex] == false) {
                        read[currentSplitIndex] = true;
                    } else {
                        continue;
                    }
                }
                if (snapshot != null) {
                    String fileName = ((FileSplit) (inputSplits[currentSplitIndex])).getPath().toUri().getPath();
                    FileStatus fileStatus = hdfs.getFileStatus(new Path(fileName));
                    // Skip if not the same file stored in the files snapshot
                    if (fileStatus.getModificationTime() != snapshot.get(currentSplitIndex).getLastModefiedTime()
                            .getTime()) {
                        continue;
                    }
                }

                reader.close();
                reader = getRecordReader(currentSplitIndex);
                return true;
            }
        }
        return false;
    }

    @SuppressWarnings("unchecked")
    private RecordReader<K, Writable> getRecordReader(int splitIndex) throws IOException {
        reader = (RecordReader<K, Writable>) inputFormat.getRecordReader(inputSplits[splitIndex], conf, Reporter.NULL);
        if (key == null) {
            key = reader.createKey();
            value = (V) reader.createValue();
        }
        if (indexer != null) {
            try {
                indexer.reset(this);
            } catch (Exception e) {
                throw HyracksDataException.create(e);
            }
        }
        return reader;
    }

    @Override
    public boolean stop() {
        return false;
    }

    @Override
    public IExternalIndexer getIndexer() {
        return indexer;
    }

    @Override
    public List<ExternalFile> getSnapshot() {
        return snapshot;
    }

    @Override
    public int getCurrentSplitIndex() {
        return currentSplitIndex;
    }

    public RecordReader<K, Writable> getReader() {
        return reader;
    }

    @Override
    public void setFeedLogManager(FeedLogManager feedLogManager) {
    }

    @Override
    public void setController(AbstractFeedDataFlowController controller) {
    }

    @Override
    public boolean handleException(Throwable th) {
        return false;
    }
}
