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
package org.apache.asterix.cloud;

import java.io.IOException;

import org.apache.asterix.cloud.clients.ICloudWriter;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.cloud.filesystem.FileSystemOperationDispatcherUtil;
import org.apache.hyracks.control.nc.io.FileHandle;

public class CloudFileHandle extends FileHandle {
    private final ICloudWriter cloudWriter;
    private int blockSize;
    private int fileDescriptor;

    public CloudFileHandle(FileReference fileRef, ICloudWriter cloudWriter) {
        super(fileRef);
        this.cloudWriter = cloudWriter;
    }

    @Override
    public void open(IIOManager.FileReadWriteMode rwMode, IIOManager.FileSyncMode syncMode) throws IOException {
        if (fileRef.getFile().exists()) {
            super.open(rwMode, syncMode);
        }
        fileDescriptor = FileSystemOperationDispatcherUtil.getFileDescriptor(getFileChannel());
        blockSize = FileSystemOperationDispatcherUtil.getBlockSize(fileDescriptor);
    }

    public ICloudWriter getCloudWriter() {
        return cloudWriter;
    }

    public int getBlockSize() throws HyracksDataException {
        return blockSize;
    }

    public int getFileDescriptor() throws HyracksDataException {
        return fileDescriptor;
    }
}
