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
package org.apache.asterix.cloud.util;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Set;

import org.apache.asterix.cloud.clients.ICloudClient;
import org.apache.commons.io.FileUtils;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IFileHandle;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.control.nc.io.FileHandle;
import org.apache.hyracks.control.nc.io.IOManager;

public class CloudFileUtil {
    private CloudFileUtil() {
    }

    public static void downloadFile(IOManager ioManager, ICloudClient cloudClient, String bucket, FileHandle fileHandle,
            IIOManager.FileReadWriteMode rwMode, IIOManager.FileSyncMode syncMode, ByteBuffer writeBuffer)
            throws HyracksDataException {
        FileReference fileRef = fileHandle.getFileReference();
        File file = fileRef.getFile();

        try (InputStream inputStream = cloudClient.getObjectStream(bucket, fileRef.getRelativePath())) {
            FileUtils.createParentDirectories(file);
            if (!file.createNewFile()) {
                throw new IllegalStateException("Couldn't create local file");
            }

            fileHandle.open(rwMode, syncMode);
            writeToFile(ioManager, fileHandle, inputStream, writeBuffer);
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    public static void cleanDirectoryFiles(IOManager ioManager, Set<String> cloudFiles, FileReference partitionPath)
            throws HyracksDataException {
        // First get the set of local files
        Set<FileReference> localFiles = ioManager.list(partitionPath);
        Iterator<FileReference> localFilesIter = localFiles.iterator();

        // Reconcile local files and cloud files
        while (localFilesIter.hasNext()) {
            FileReference file = localFilesIter.next();
            if (file.getFile().isDirectory()) {
                continue;
            }

            String path = file.getRelativePath();
            if (!cloudFiles.contains(path)) {
                // Delete local files that do not exist in cloud storage (the ground truth for valid files)
                localFilesIter.remove();
                ioManager.delete(file);
            } else {
                // No need to re-add it in the following loop
                cloudFiles.remove(path);
            }
        }

        // Add the remaining files that are not stored locally (if any)
        for (String cloudFile : cloudFiles) {
            if (!cloudFile.contains(partitionPath.getRelativePath())) {
                continue;
            }
            localFiles.add(new FileReference(partitionPath.getDeviceHandle(),
                    cloudFile.substring(cloudFile.indexOf(partitionPath.getRelativePath()))));
        }
    }

    private static void writeToFile(IOManager ioManager, IFileHandle fileHandle, InputStream inStream,
            ByteBuffer writeBuffer) throws HyracksDataException {
        writeBuffer.clear();
        try {
            int position = 0;
            long offset = 0;
            int read;
            while ((read = inStream.read(writeBuffer.array(), position, writeBuffer.remaining())) >= 0) {
                position += read;
                writeBuffer.position(position);
                if (writeBuffer.remaining() == 0) {
                    offset += writeBufferToFile(ioManager, fileHandle, writeBuffer, offset);
                    position = 0;
                }
            }

            if (writeBuffer.position() > 0) {
                writeBufferToFile(ioManager, fileHandle, writeBuffer, offset);
                ioManager.sync(fileHandle, true);
            }
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    private static long writeBufferToFile(IOManager ioManager, IFileHandle fileHandle, ByteBuffer writeBuffer,
            long offset) throws HyracksDataException {
        writeBuffer.flip();
        long written = ioManager.doSyncWrite(fileHandle, offset, writeBuffer);
        writeBuffer.clear();
        return written;
    }
}
