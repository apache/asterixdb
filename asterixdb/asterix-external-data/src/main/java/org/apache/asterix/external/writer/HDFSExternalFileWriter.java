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
package org.apache.asterix.external.writer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.external.util.HDFSUtils;
import org.apache.asterix.runtime.writer.IExternalFileWriter;
import org.apache.asterix.runtime.writer.IExternalPrinter;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.parquet.hadoop.util.HiddenFileFilter;

public class HDFSExternalFileWriter implements IExternalFileWriter {

    private final IExternalPrinter printer;
    private final FileSystem fs;
    private final boolean partitionedPath;
    private final SourceLocation pathSourceLocation;
    private FSDataOutputStream outputStream = null;
    private final List<Path> paths = new ArrayList<>();

    HDFSExternalFileWriter(IExternalPrinter printer, FileSystem fs, boolean partitionedPath,
            SourceLocation pathSourceLocation) {
        this.printer = printer;
        this.fs = fs;
        this.partitionedPath = partitionedPath;
        this.pathSourceLocation = pathSourceLocation;
    }

    @Override
    public void open() throws HyracksDataException {
        printer.open();
    }

    @Override
    public void validate(String directory) throws HyracksDataException {
        if (partitionedPath) {
            directory = HDFSUtils.updateRootPath(directory, true);
            Path dirPath = new Path(directory);
            try {
                if (fs.exists(dirPath)) {
                    FileStatus fileStatus = fs.getFileStatus(dirPath);
                    if (fileStatus.isFile()) {
                        throw new RuntimeDataException(ErrorCode.DIRECTORY_IS_NOT_EMPTY, pathSourceLocation, directory);
                    }
                    if (fileStatus.isDirectory()) {
                        FileStatus[] fileStatuses = fs.listStatus(dirPath, HiddenFileFilter.INSTANCE);
                        if (fileStatuses.length != 0) {
                            throw new RuntimeDataException(ErrorCode.DIRECTORY_IS_NOT_EMPTY, pathSourceLocation,
                                    directory);
                        }
                    }
                }
            } catch (IOException ex) {
                throw HyracksDataException.create(ex);
            }
        }
    }

    @Override
    public boolean newFile(String directory, String fileName) throws HyracksDataException {
        directory = HDFSUtils.updateRootPath(directory, true);
        Path path = new Path(directory, "." + fileName);
        try {
            outputStream = fs.create(path, false);
            paths.add(path);
            printer.newStream(outputStream);
        } catch (FileAlreadyExistsException e) {
            return false;
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
        return true;
    }

    @Override
    public void write(IValueReference value) throws HyracksDataException {
        printer.print(value);
    }

    @Override
    public void abort() throws HyracksDataException {
        try {
            printer.close();
            for (Path path : paths) {
                fs.delete(path, false);
            }
        } catch (IOException ex) {
            throw HyracksDataException.create(ex);
        }
    }

    @Override
    public void close() throws HyracksDataException {
        printer.close();
        try {
            for (Path path : paths) {
                fs.rename(path, new Path(path.getParent(), path.getName().substring(1)));
            }
        } catch (IOException ex) {
            throw HyracksDataException.create(ex);
        }
    }
}
