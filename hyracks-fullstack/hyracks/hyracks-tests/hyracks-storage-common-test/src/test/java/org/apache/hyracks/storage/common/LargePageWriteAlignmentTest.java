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

import static org.apache.hyracks.storage.common.buffercache.IBufferCache.RESERVED_HEADER_BYTES;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.storage.common.buffercache.HaltOnFailureCallback;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.buffercache.IFIFOPageWriter;
import org.apache.hyracks.storage.common.buffercache.NoOpPageWriteCallback;
import org.apache.hyracks.storage.common.buffercache.context.write.DefaultBufferCacheWriteContext;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;
import org.apache.hyracks.test.support.TestStorageManagerComponentHolder;
import org.apache.hyracks.test.support.TestUtils;
import org.apache.hyracks.util.annotations.AiProvenance;
import org.junit.Assert;
import org.junit.Test;

/**
 * A page is addressed as {@code pageId * (pageSize + RESERVED_HEADER_BYTES)}, so a page spanning N slots owns
 * {@code N * (pageSize + RESERVED_HEADER_BYTES)} bytes. Only one header is written for the whole page, so
 * {@code RESERVED_HEADER_BYTES * (N - 1)} bytes at its tail used to be left unwritten. A local file tolerates
 * that as a hole, but writes mirrored to an append-only cloud stream cannot skip forward, which is how MB-73296
 * failed: the stream fell behind the page offsets and the next write was rejected as misaligned.
 * <p>
 * These tests assert the write covers every byte of every slot it owns.
 */
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED, notes = "Assert a large page write covers every slot it owns, contiguous and non contiguous")
public class LargePageWriteAlignmentTest {

    private static final int PAGE_SIZE = 256;
    private static final int SLOT = PAGE_SIZE + RESERVED_HEADER_BYTES;
    private static final int NUM_PAGES = 40;
    private static final int MAX_OPEN_FILES = 20;
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("ddMMyy-hhmmssSS");

    private final IHyracksTaskContext ctx = TestUtils.create(PAGE_SIZE);

    private long writeLargePageAndGetFileSize(int frames) throws HyracksDataException {
        // contiguous: the extra block starts immediately after the first page, as a bulk loader produces
        return writeLargePageAndGetFileSize(frames, 1);
    }

    private long writeLargePageAndGetFileSize(int frames, int extraBlockPageId) throws HyracksDataException {
        // the buffer cache must be able to hold the whole page, plus a little slack
        TestStorageManagerComponentHolder.init(PAGE_SIZE, Math.max(NUM_PAGES, frames + extraBlockPageId + 8),
                MAX_OPEN_FILES);
        IBufferCache bufferCache =
                TestStorageManagerComponentHolder.getBufferCache(ctx.getJobletContext().getServiceContext());
        IIOManager ioManager = TestStorageManagerComponentHolder.getIOManager();
        FileReference file =
                ioManager.resolve(DATE_FORMAT.format(new Date()) + "-frames" + frames + "-extra" + extraBlockPageId);
        int fileId = bufferCache.createFile(file);
        bufferCache.openFile(fileId);
        IFIFOPageWriter writer = bufferCache.createFIFOWriter(NoOpPageWriteCallback.INSTANCE,
                HaltOnFailureCallback.INSTANCE, DefaultBufferCacheWriteContext.INSTANCE);
        // page 0 spans "frames" slots and its extra block starts at page 1, i.e. contiguous, which is what a
        // bulk loader produces via freePageManager.takeBlock
        long dpid = BufferedFileHandle.getDiskPageId(fileId, 0);
        ICachedPage page = frames > 1 ? bufferCache.confiscateLargePage(dpid, frames, extraBlockPageId)
                : bufferCache.confiscatePage(dpid);
        for (int i = 0; i < frames * PAGE_SIZE; i += Integer.BYTES) {
            page.getBuffer().putInt(i, i);
        }
        writer.write(page);
        bufferCache.closeFile(fileId);
        long size = ioManager.getSize(ioManager.open(file, IIOManager.FileReadWriteMode.READ_ONLY,
                IIOManager.FileSyncMode.METADATA_ASYNC_DATA_ASYNC));
        bufferCache.deleteFile(fileId);
        return size;
    }

    @Test
    public void anOrdinaryPageFillsItsSlot() throws HyracksDataException {
        Assert.assertEquals(SLOT, writeLargePageAndGetFileSize(1));
    }

    @Test
    public void aLargePageFillsEverySlotItOwns() throws HyracksDataException {
        for (int frames = 2; frames <= 8; frames++) {
            long expected = (long) frames * SLOT;
            long actual = writeLargePageAndGetFileSize(frames);
            Assert.assertEquals("a " + frames + " frame page must cover all " + frames + " slots; without the tail "
                    + "it stops " + (RESERVED_HEADER_BYTES * (frames - 1)) + " bytes short", expected, actual);
        }
    }

    @Test
    public void theTailIsExactlyTheMissingHeaders() throws HyracksDataException {
        int frames = 4;
        long payloadEnd = RESERVED_HEADER_BYTES + (long) frames * PAGE_SIZE;
        long slotsEnd = (long) frames * SLOT;
        Assert.assertEquals(RESERVED_HEADER_BYTES * (frames - 1), slotsEnd - payloadEnd);
        // the numbers from MB-73296, scaled to this page size
        Assert.assertEquals(slotsEnd, writeLargePageAndGetFileSize(frames));
    }

    @Test
    public void aPageWithMoreFramesThanTheSharedZeroBufferStillFillsItsSlots() throws HyracksDataException {
        // the shared zero buffer holds RESERVED_HEADER_BYTES * 1024 bytes, so a page needing a longer tail
        // exercises the chunking loop
        int frames = 1200;
        Assert.assertTrue("this must exceed the shared buffer to be meaningful",
                (long) RESERVED_HEADER_BYTES * (frames - 1) > RESERVED_HEADER_BYTES * 1024);
        Assert.assertEquals((long) frames * SLOT, writeLargePageAndGetFileSize(frames));
    }

    // ---- non contiguous large pages: the extra block lives elsewhere in the file, so the tail belongs after
    // ---- the extra block rather than after the first page

    @Test
    public void aNonContiguousLargePageFillsTheSlotsItOwns() throws HyracksDataException {
        int frames = 4;
        int extraBlockPageId = 5;
        // slot 0 holds the header and the first page; the remaining frames sit at slot 5 onwards, and the tail
        // follows them, so the file ends at slot (extraBlockPageId + frames - 1)
        long expected = (long) (extraBlockPageId + frames - 1) * SLOT;
        Assert.assertEquals(expected, writeLargePageAndGetFileSize(frames, extraBlockPageId));
    }

    @Test
    public void aNonContiguousTwoFrameLargePageFillsTheSlotsItOwns() throws HyracksDataException {
        int frames = 2;
        int extraBlockPageId = 3;
        Assert.assertEquals((long) (extraBlockPageId + frames - 1) * SLOT,
                writeLargePageAndGetFileSize(frames, extraBlockPageId));
    }
}
