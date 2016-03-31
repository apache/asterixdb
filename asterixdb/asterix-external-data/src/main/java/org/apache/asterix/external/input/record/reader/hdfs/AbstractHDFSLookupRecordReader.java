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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;

import org.apache.asterix.external.api.ILookupRecordReader;
import org.apache.asterix.external.api.IRawRecord;
import org.apache.asterix.external.indexing.ExternalFile;
import org.apache.asterix.external.indexing.ExternalFileIndexAccessor;
import org.apache.asterix.external.indexing.RecordId;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public abstract class AbstractHDFSLookupRecordReader<T> implements ILookupRecordReader<T> {

    protected int fileId;
    private ExternalFileIndexAccessor snapshotAccessor;
    protected ExternalFile file;
    protected FileSystem fs;
    protected Configuration conf;
    protected boolean replaced;

    public AbstractHDFSLookupRecordReader(ExternalFileIndexAccessor snapshotAccessor, FileSystem fs,
            Configuration conf) {
        this.snapshotAccessor = snapshotAccessor;
        this.fs = fs;
        this.conf = conf;
        this.fileId = -1;
        this.file = new ExternalFile();
    }

    @Override
    public void configure(Map<String, String> configurations) throws Exception {
    }

    @Override
    public IRawRecord<T> read(RecordId rid) throws Exception {
        if (rid.getFileId() != fileId) {
            // close current file
            closeFile();
            // lookup new file
            snapshotAccessor.lookup(rid.getFileId(), file);
            fileId = rid.getFileId();
            try {
                validate();
                if (!replaced) {
                    openFile();
                    validate();
                    if (replaced) {
                        closeFile();
                    }
                }
            } catch (FileNotFoundException e) {
                replaced = true;
            }
        }
        if (replaced) {
            return null;
        }
        return lookup(rid);
    }

    protected abstract IRawRecord<T> lookup(RecordId rid) throws IOException;

    private void validate() throws IllegalArgumentException, IOException {
        FileStatus fileStatus = fs.getFileStatus(new Path(file.getFileName()));
        replaced = fileStatus.getModificationTime() != file.getLastModefiedTime().getTime();
    }

    protected abstract void closeFile();

    protected abstract void openFile() throws IllegalArgumentException, IOException;

    @Override
    public final void open() throws HyracksDataException {
        snapshotAccessor.open();
    }

    @Override
    public void close() throws IOException {
        try {
            closeFile();
        } finally {
            snapshotAccessor.close();
        }
    }

    @Override
    public void fail() {
    }
}
