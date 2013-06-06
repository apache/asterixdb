/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.storage.common;

import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.Test;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.io.IFileHandle;
import edu.uci.ics.hyracks.api.io.IIOManager;
import edu.uci.ics.hyracks.api.io.IIOManager.FileReadWriteMode;
import edu.uci.ics.hyracks.api.io.IIOManager.FileSyncMode;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.ICachedPage;
import edu.uci.ics.hyracks.storage.common.file.BufferedFileHandle;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;
import edu.uci.ics.hyracks.test.support.TestStorageManagerComponentHolder;
import edu.uci.ics.hyracks.test.support.TestUtils;

public class BufferCacheRegressionTest {
    protected static final String tmpDir = System.getProperty("java.io.tmpdir");
    protected static final String sep = System.getProperty("file.separator");

    protected String fileName = tmpDir + sep + "flushTestFile";

    private static final int PAGE_SIZE = 256;
    private static final int HYRACKS_FRAME_SIZE = PAGE_SIZE;
    private IHyracksTaskContext ctx = TestUtils.create(HYRACKS_FRAME_SIZE);

    // We want to test the following behavior when reclaiming a file slot in the
    // buffer cache:
    // 1. If the file being evicted was deleted, then its dirty pages should be
    // invalidated, but most not be flushed.
    // 2. If the file was not deleted, then we must flush its dirty pages.
    @Test
    public void testFlushBehaviorOnFileEviction() throws IOException {
        File f = new File(fileName);
        if (f.exists()) {
            f.delete();
        }
        flushBehaviorTest(true);
        flushBehaviorTest(false);
    }

    private void flushBehaviorTest(boolean deleteFile) throws IOException {
        TestStorageManagerComponentHolder.init(PAGE_SIZE, 10, 1);

        IBufferCache bufferCache = TestStorageManagerComponentHolder.getBufferCache(ctx);
        IFileMapProvider fmp = TestStorageManagerComponentHolder.getFileMapProvider(ctx);

        FileReference firstFileRef = new FileReference(new File(fileName));
        bufferCache.createFile(firstFileRef);
        int firstFileId = fmp.lookupFileId(firstFileRef);
        bufferCache.openFile(firstFileId);

        // Fill the first page with known data and make it dirty by write
        // latching it.
        ICachedPage writePage = bufferCache.pin(BufferedFileHandle.getDiskPageId(firstFileId, 0), true);
        writePage.acquireWriteLatch();
        try {
            ByteBuffer buf = writePage.getBuffer();
            for (int i = 0; i < buf.capacity(); i++) {
                buf.put(Byte.MAX_VALUE);
            }
        } finally {
            writePage.releaseWriteLatch();
            bufferCache.unpin(writePage);
        }
        bufferCache.closeFile(firstFileId);
        if (deleteFile) {
            bufferCache.deleteFile(firstFileId, false);
        }

        // Create a file with the same name.
        FileReference secondFileRef = new FileReference(new File(fileName));
        bufferCache.createFile(secondFileRef);
        int secondFileId = fmp.lookupFileId(secondFileRef);

        // This open will replace the firstFileRef's slot in the BufferCache,
        // causing it's pages to be cleaned up. We want to make sure that those
        // dirty pages are not flushed to the disk, because the file was
        // declared as deleted, and
        // somebody might be already using the same filename again (having been
        // assigned a different fileId).
        bufferCache.openFile(secondFileId);

        // Manually open the file and inspect it's contents. We cannot simply
        // ask the BufferCache to pin the page, because it would return the same
        // physical memory again, and for performance reasons pages are never
        // reset with 0's.
        IIOManager ioManager = ctx.getIOManager();
        FileReference testFileRef = new FileReference(new File(fileName));
        IFileHandle testFileHandle = ioManager.open(testFileRef, FileReadWriteMode.READ_ONLY,
                FileSyncMode.METADATA_SYNC_DATA_SYNC);
        ByteBuffer testBuffer = ByteBuffer.allocate(PAGE_SIZE);
        ioManager.syncRead(testFileHandle, 0, testBuffer);
        for (int i = 0; i < testBuffer.capacity(); i++) {
            if (deleteFile) {
                // We deleted the file. We expect to see a clean buffer.
                if (testBuffer.get(i) == Byte.MAX_VALUE) {
                    fail("Page 0 of deleted file was fazily flushed in openFile(), "
                            + "corrupting the data of a newly created file with the same name.");
                }
            } else {
                // We didn't delete the file. We expect to see a buffer full of
                // Byte.MAX_VALUE.
                if (testBuffer.get(i) != Byte.MAX_VALUE) {
                    fail("Page 0 of closed file was not flushed when properly, when reclaiming the file slot of fileId 0 in the BufferCache.");
                }
            }
        }
        ioManager.close(testFileHandle);
        bufferCache.closeFile(secondFileId);
        if (deleteFile) {
            bufferCache.deleteFile(secondFileId, false);
        }
        bufferCache.close();
    }
}
