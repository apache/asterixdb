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

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.RCFile.Reader;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import org.apache.asterix.common.config.DatasetConfig.ExternalFilePendingOp;
import org.apache.asterix.metadata.entities.ExternalFile;
import org.apache.asterix.metadata.external.ExternalFileIndexAccessor;

public class RCFileLookupReader {
    private FileSystem fs;
    private Configuration conf;
    private int fileNumber = -1;
    private int rowNumber;
    private long recordGroupOffset;
    private Reader reader;
    boolean skipFile = false;
    private LongWritable rcKey = new LongWritable();
    private BytesRefArrayWritable rcValue = new BytesRefArrayWritable();
    private ExternalFile currentFile = new ExternalFile(null, null, 0, null, null, 0L,
            ExternalFilePendingOp.PENDING_NO_OP);
    private ExternalFileIndexAccessor filesIndexAccessor;

    public RCFileLookupReader(ExternalFileIndexAccessor filesIndexAccessor, Configuration conf) throws IOException {
        fs = FileSystem.get(conf);
        this.conf = conf;
        this.filesIndexAccessor = filesIndexAccessor;
    }

    public Writable read(int fileNumber, long recordGroupOffset, int rowNumber) throws Exception {
        if (fileNumber != this.fileNumber) {
            filesIndexAccessor.searchForFile(fileNumber, currentFile);
            try {
                FileStatus fileStatus = fs.getFileStatus(new Path(currentFile.getFileName()));
                if (fileStatus.getModificationTime() != currentFile.getLastModefiedTime().getTime()) {
                    this.fileNumber = fileNumber;
                    skipFile = true;
                    return null;
                } else {
                    this.fileNumber = fileNumber;
                    skipFile = false;
                }
            } catch (FileNotFoundException e) {
                // Couldn't find file, skip it
                this.fileNumber = fileNumber;
                skipFile = true;
                return null;
            }
            // Close old file and open new one
            if (reader != null)
                reader.close();
            reader = new Reader(fs, new Path(currentFile.getFileName()), conf);
            this.recordGroupOffset = -1;
            this.rowNumber = -1;
        } else if (skipFile) {
            return null;
        }
        // Seek to the record group if needed
        if (recordGroupOffset != this.recordGroupOffset) {
            this.recordGroupOffset = recordGroupOffset;
            if(reader.getPosition() != recordGroupOffset)
                reader.seek(recordGroupOffset);
            reader.resetBuffer();
            this.rowNumber = -1;
        }

        // skip rows to the record row
        while (this.rowNumber < rowNumber) {
            reader.next(rcKey);
            reader.getCurrentRow(rcValue);
            this.rowNumber++;
        }
        return rcValue;
    }

    public void close() throws Exception {
        if (reader != null)
            reader.close();
    }
}
