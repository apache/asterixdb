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
import java.io.InputStream;

import org.apache.asterix.common.config.DatasetConfig.ExternalFilePendingOp;
import org.apache.asterix.metadata.entities.ExternalFile;
import org.apache.asterix.metadata.external.ExternalFileIndexAccessor;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

/*
 * This class is used for seek and read of external data of format adm or delimited text in sequence of text input format
 */
public abstract class AbstractHDFSLookupInputStream extends InputStream {

    protected String pendingValue = null;
    protected FileSystem fs;
    protected int fileNumber = -1;
    protected int EOL = "\n".getBytes()[0];
    protected boolean skipFile = false;
    protected ExternalFile file = new ExternalFile(null, null, 0, null, null, 0, ExternalFilePendingOp.PENDING_NO_OP);
    protected ExternalFileIndexAccessor filesIndexAccessor;

    public AbstractHDFSLookupInputStream(ExternalFileIndexAccessor filesIndexAccessor, JobConf conf)
            throws IOException {
        this.filesIndexAccessor = filesIndexAccessor;
        fs = FileSystem.get(conf);
    }

    @Override
    public int read(byte[] buffer, int offset, int len) throws IOException {
        if (pendingValue != null) {
            int size = pendingValue.length() + 1;
            if (size > len) {
                return 0;
            }
            System.arraycopy(pendingValue.getBytes(), 0, buffer, offset, pendingValue.length());
            buffer[offset + pendingValue.length()] = (byte) EOL;
            pendingValue = null;
            return size;
        }
        return -1;
    }

    public boolean fetchRecord(int fileNumber, long recordOffset) throws Exception {
        if (fileNumber != this.fileNumber) {
            // New file number
            this.fileNumber = fileNumber;
            filesIndexAccessor.searchForFile(fileNumber, file);

            try {
                FileStatus fileStatus = fs.getFileStatus(new Path(file.getFileName()));
                if (fileStatus.getModificationTime() != file.getLastModefiedTime().getTime()) {
                    this.fileNumber = fileNumber;
                    skipFile = true;
                    return false;
                } else {
                    this.fileNumber = fileNumber;
                    skipFile = false;
                    openFile(file.getFileName());
                }
            } catch (FileNotFoundException e) {
                // We ignore File not found exceptions <- it means file was deleted and so we don't care about it anymore ->
                this.fileNumber = fileNumber;
                skipFile = true;
                return false;
            }
        } else if (skipFile) {
            return false;
        }
        return read(recordOffset);
    }

    @Override
    public int read() throws IOException {
        return 0;
    }

    protected abstract boolean read(long byteLocation);

    protected abstract void openFile(String fileName) throws IOException;

    @Override
    public void close() throws IOException {
        super.close();
    }

    public ExternalFileIndexAccessor getExternalFileIndexAccessor() {
        return filesIndexAccessor;
    }

    public void setExternalFileIndexAccessor(ExternalFileIndexAccessor filesIndexAccessor) {
        this.filesIndexAccessor = filesIndexAccessor;
    }
}
