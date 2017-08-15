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
package org.apache.hyracks.storage.common;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.commons.io.FileUtils;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.api.util.IoUtil;
import org.apache.hyracks.control.nc.io.FileHandle;
import org.apache.hyracks.test.support.TestStorageManagerComponentHolder;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;

public class IOManagerTest {

    private static File testFile;

    @Test
    public void interruptedReadTest() throws IOException, InterruptedException {
        final IIOManager ioManager = TestStorageManagerComponentHolder.getIOManager();
        final String fileName = String.valueOf(System.currentTimeMillis());
        final FileReference fileRef = ioManager.resolve(fileName);
        testFile = fileRef.getFile();
        // create the file
        IoUtil.create(fileRef);
        // open file handle
        final FileHandle fileHandle = (FileHandle) ioManager.open(fileRef, IIOManager.FileReadWriteMode.READ_WRITE,
                IIOManager.FileSyncMode.METADATA_ASYNC_DATA_ASYNC);
        int theOnlyOne = 1;
        // write integer into the file
        final ByteBuffer writeBuffer = ByteBuffer.allocate(Integer.BYTES).putInt(theOnlyOne);
        writeBuffer.flip();
        ioManager.syncWrite(fileHandle, 0, writeBuffer);

        final ByteBuffer readBuffer = ByteBuffer.allocate(Integer.BYTES);
        Thread interruptedReader = new Thread(() -> {
            try {
                Thread.currentThread().interrupt();
                // The file handle will be closed by ClosedByInterruptException
                ioManager.syncRead(fileHandle, 0, readBuffer);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        interruptedReader.start();
        interruptedReader.join();
        // we should still be able to read from the file handle
        ioManager.syncRead(fileHandle, 0, readBuffer);
        Assert.assertEquals(theOnlyOne, readBuffer.getInt(0));
    }

    @AfterClass
    public static void cleanup() throws Exception {
        FileUtils.deleteQuietly(testFile);
    }
}
