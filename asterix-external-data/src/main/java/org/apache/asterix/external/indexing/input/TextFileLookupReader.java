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
import org.apache.hadoop.io.Text;

import org.apache.asterix.common.config.DatasetConfig.ExternalFilePendingOp;
import org.apache.asterix.metadata.entities.ExternalFile;
import org.apache.asterix.metadata.external.ExternalFileIndexAccessor;

public class TextFileLookupReader implements ILookupReader {
    private FileSystem fs;
    private int fileNumber = -1;
    private boolean skipFile = false;
    private ExternalFile file = new ExternalFile(null, null, 0, null, null, 0L, ExternalFilePendingOp.PENDING_NO_OP);
    private ExternalFileIndexAccessor filesIndexAccessor;
    private HDFSSeekableLineReader lineReader;
    private Text value = new Text();

    public TextFileLookupReader(ExternalFileIndexAccessor filesIndexAccessor, Configuration conf) throws IOException {
        this.fs = FileSystem.get(conf);
        this.filesIndexAccessor = filesIndexAccessor;
        this.lineReader = new HDFSSeekableLineReader();
    }

    @Override
    public String read(int fileNumber, long recordOffset) throws Exception {
        if (fileNumber != this.fileNumber) {
            this.fileNumber = fileNumber;
            filesIndexAccessor.searchForFile(fileNumber, file);

            try {
                FileStatus fileStatus = fs.getFileStatus(new Path(file.getFileName()));
                if (fileStatus.getModificationTime() != file.getLastModefiedTime().getTime()) {
                    this.fileNumber = fileNumber;
                    skipFile = true;
                    return null;
                } else {
                    this.fileNumber = fileNumber;
                    skipFile = false;
                    openFile(file.getFileName());
                }
            } catch (FileNotFoundException e) {
                // File is not there, skip it and do nothing
                this.fileNumber = fileNumber;
                skipFile = true;
                return null;
            }
        } else if (skipFile) {
            return null;
        }
        lineReader.seek(recordOffset);
        lineReader.readLine(value);
        return value.toString();
    }

    private void openFile(String FileName) throws IOException {
        if(lineReader.getReader() != null){
            lineReader.getReader().close();
        }
        lineReader.resetReader(fs.open(new Path(FileName)));
    }

    public void close() {
        if (lineReader.getReader() != null){
            try {
                lineReader.getReader().close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
