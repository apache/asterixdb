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

import org.apache.asterix.metadata.entities.ExternalFile;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

/**
 * This is a generic reader used for indexing external dataset or for performing full scan for external dataset with
 * a stored snapshot
 *
 * @author alamouda
 */

public class GenericFileAwareRecordReader extends GenericRecordReader {

    private List<ExternalFile> files;
    private FileSystem hadoopFS;
    private long recordOffset = 0L;

    public GenericFileAwareRecordReader(InputSplit[] inputSplits, String[] readSchedule, String nodeName, JobConf conf,
            boolean[] executed, List<ExternalFile> files) throws IOException {
        super(inputSplits, readSchedule, nodeName, conf, executed);
        this.files = files;
        hadoopFS = FileSystem.get(conf);
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
                try {
                    String fileName = ((FileSplit) (inputSplits[currentSplitIndex])).getPath().toUri().getPath();
                    FileStatus fileStatus = hadoopFS.getFileStatus(new Path(fileName));
                    //skip if not the same file stored in the files snapshot
                    if (fileStatus.getModificationTime() != files.get(currentSplitIndex).getLastModefiedTime()
                            .getTime())
                        continue;
                    reader = getRecordReader(currentSplitIndex);
                } catch (Exception e) {
                    continue;
                }
                key = reader.createKey();
                value = reader.createValue();
                return true;
            }
        }
        return false;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object readNext() throws IOException {

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
    public String getFileName() throws Exception {
        return files.get(currentSplitIndex).getFileName();
    }

    @Override
    public long getReaderPosition() throws Exception {
        return recordOffset;
    }

    @Override
    public int getFileNumber() throws Exception {
        return files.get(currentSplitIndex).getFileNumber();
    }

}
