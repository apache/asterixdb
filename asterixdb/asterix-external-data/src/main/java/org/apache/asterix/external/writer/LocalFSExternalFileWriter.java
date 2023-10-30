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

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.runtime.writer.IExternalFilePrinter;
import org.apache.asterix.runtime.writer.IExternalFileWriter;
import org.apache.commons.io.FileUtils;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.api.IValueReference;

final class LocalFSExternalFileWriter implements IExternalFileWriter {
    private final IExternalFilePrinter printer;
    private final ILocalFSValidator validator;
    private final SourceLocation pathSourceLocation;

    LocalFSExternalFileWriter(IExternalFilePrinter printer, ILocalFSValidator validator,
            SourceLocation pathSourceLocation) {
        this.printer = printer;
        this.validator = validator;
        this.pathSourceLocation = pathSourceLocation;
    }

    @Override
    public void open() throws HyracksDataException {
        printer.open();
    }

    @Override
    public void validate(String directory) throws HyracksDataException {
        validator.validate(directory, pathSourceLocation);
    }

    @Override
    public boolean newFile(String directory, String fileName) throws HyracksDataException {
        try {
            File parentDirectory = new File(directory);
            File currentFile = new File(parentDirectory, fileName);
            FileUtils.createParentDirectories(currentFile);
            if (!currentFile.createNewFile()) {
                throw RuntimeDataException.create(ErrorCode.COULD_NOT_CREATE_FILE, currentFile.getAbsolutePath());
            }
            printer.newStream(new BufferedOutputStream(new FileOutputStream(currentFile)));
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
        printer.close();
    }

    @Override
    public void close() throws HyracksDataException {
        printer.close();
    }
}
