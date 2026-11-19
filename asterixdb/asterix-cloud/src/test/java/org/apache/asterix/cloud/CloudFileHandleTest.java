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

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.file.Files;

import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.api.io.IODeviceHandle;
import org.apache.hyracks.api.util.IoUtil;
import org.apache.hyracks.cloud.filesystem.FileSystemOperationDispatcherUtil;
import org.junit.Assert;
import org.junit.Test;

public class CloudFileHandleTest {

    @Test
    public void reopenRefreshesFileDescriptor() throws Exception {
        File dir = Files.createTempDirectory("cloud-file-handle").toFile();
        try {
            File data = new File(dir, "data");
            File other = new File(dir, "other");
            Assert.assertTrue(data.createNewFile());
            Assert.assertTrue(other.createNewFile());
            CloudFileHandle handle =
                    new CloudFileHandle(new FileReference(new IODeviceHandle(dir, "."), data.getName()), null);
            handle.open(IIOManager.FileReadWriteMode.READ_WRITE, IIOManager.FileSyncMode.METADATA_ASYNC_DATA_ASYNC);
            try {
                handle.close();
                // takes the number the closed channel gave up, where the OS reuses the lowest free descriptor
                try (RandomAccessFile squatter = new RandomAccessFile(other, "rw")) {
                    handle.ensureOpen();
                    int live = FileSystemOperationDispatcherUtil.getFileDescriptor(handle.getFileChannel());
                    Assert.assertEquals(live, handle.getFileDescriptor());
                    if (live >= 0) {
                        int squatterFd = FileSystemOperationDispatcherUtil.getFileDescriptor(squatter.getChannel());
                        Assert.assertNotEquals(squatterFd, handle.getFileDescriptor());
                    }
                }
            } finally {
                handle.close();
            }
        } finally {
            IoUtil.delete(dir);
        }
    }

    @Test
    public void pendingUploadKeepsTheDescriptor() {
        CloudFileHandle handle = new CloudFileHandle(null, null);
        Assert.assertTrue(handle.canReleaseDescriptor());
        handle.uploadWritten();
        Assert.assertFalse(handle.canReleaseDescriptor());
        handle.uploadFinished();
        Assert.assertTrue(handle.canReleaseDescriptor());
    }
}
