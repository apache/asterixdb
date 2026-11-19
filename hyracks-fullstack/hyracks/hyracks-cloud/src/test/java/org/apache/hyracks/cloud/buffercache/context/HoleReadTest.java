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
package org.apache.hyracks.cloud.buffercache.context;

import static org.apache.hyracks.cloud.test.RowFileFixture.FRAMES;
import static org.apache.hyracks.cloud.test.RowFileFixture.LARGE_PAGE_ID;
import static org.apache.hyracks.cloud.test.RowFileFixture.PAGE_SIZE;
import static org.apache.hyracks.cloud.test.RowFileFixture.SLOT;
import static org.apache.hyracks.cloud.test.RowFileFixture.cloudContext;
import static org.apache.hyracks.cloud.test.RowFileFixture.drive;
import static org.apache.hyracks.cloud.test.RowFileFixture.fresh;
import static org.apache.hyracks.cloud.test.RowFileFixture.largePage;
import static org.apache.hyracks.cloud.test.RowFileFixture.ordinaryPage;
import static org.apache.hyracks.cloud.test.RowFileFixture.read;
import static org.apache.hyracks.storage.common.buffercache.IBufferCache.RESERVED_HEADER_BYTES;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.Collections;
import java.util.List;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IODeviceHandle;
import org.apache.hyracks.cloud.test.FakeCloudIOManager;
import org.apache.hyracks.cloud.test.RowFileFixture;
import org.apache.hyracks.storage.common.buffercache.BufferCache;
import org.apache.hyracks.storage.common.buffercache.context.IBufferCacheReadContext;
import org.apache.hyracks.storage.common.buffercache.context.read.DefaultBufferCacheReadContextProvider;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Reading pages that are holes in the local file from the cloud storage.
 * <p>
 * A large page carries a header only on its first page, so when its local copy is a hole its extra pages cannot be
 * recognised as holes by themselves. Reading them from the local file returned zeros in place of the page's tail,
 * and once the first page was persisted, every later read did the same.
 * <p>
 * Reading ahead fetches the pages right after a missing one in the same request and saves them, except a large
 * page, whose header must not be saved before all of its extra pages.
 */
public class HoleReadTest {
    private static final int CONTIGUOUS = LARGE_PAGE_ID + 1;
    private static final int NON_CONTIGUOUS = 5;

    @Rule
    public final TemporaryFolder folder = new TemporaryFolder();

    private FakeCloudIOManager ioManager;
    private RowFileFixture fixture;

    @Before
    public void setUp() throws HyracksDataException {
        ioManager =
                new FakeCloudIOManager(Collections.singletonList(new IODeviceHandle(folder.getRoot(), "workspace")));
        fixture = new RowFileFixture(ioManager);
    }

    @After
    public void tearDown() throws IOException {
        ioManager.close();
    }

    @Test
    public void contiguousLargePageIsReadWhole() throws Exception {
        assertReadWholeAndPersisted(false, CONTIGUOUS);
    }

    @Test
    public void nonContiguousLargePageIsReadWhole() throws Exception {
        assertReadWholeAndPersisted(false, NON_CONTIGUOUS);
    }

    @Test
    public void compressedContiguousLargePageIsReadWhole() throws Exception {
        assertReadWholeAndPersisted(true, CONTIGUOUS);
    }

    @Test
    public void compressedNonContiguousLargePageIsReadWhole() throws Exception {
        assertReadWholeAndPersisted(true, NON_CONTIGUOUS);
    }

    @Test
    public void largePageIsReadWholeWithoutPersistingWhenTheDiskHasNoSpace() throws Exception {
        for (boolean compressed : new boolean[] { false, true }) {
            FileReference file = fixture.writeAndHole(compressed, NON_CONTIGUOUS);
            ioManager.localWrites.clear();
            Assert.assertArrayEquals(largePage(), readLargePage(file, cloudContext(false)));
            Assert.assertTrue("nothing may be written locally", ioManager.localWrites.isEmpty());
            byte[] local = Files.readAllBytes(file.getFile().toPath());
            Assert.assertArrayEquals("the local copy must still be a hole", new byte[local.length], local);
        }
    }

    @Test
    public void anOrdinaryPageIsStillPersistedBodyThenHeader() throws Exception {
        FileReference file = fixture.writeAndHole(false, CONTIGUOUS);
        ioManager.localWrites.clear();
        BufferCache bufferCache = fixture.createBufferCache(cloudContext(true));
        try {
            int fileId = bufferCache.openFile(fresh(file));
            Assert.assertArrayEquals(ordinaryPage(0), read(bufferCache, fileId, 0, 1));
            bufferCache.closeFile(fileId);
        } finally {
            bufferCache.close();
        }
        Assert.assertEquals(2, ioManager.localWrites.size());
        Assert.assertArrayEquals(new long[] { RESERVED_HEADER_BYTES, PAGE_SIZE }, ioManager.localWrites.get(0));
        Assert.assertArrayEquals(new long[] { 0, RESERVED_HEADER_BYTES }, ioManager.localWrites.get(1));
    }

    @Test
    public void aContextThatCannotReadLargeHolesFailsInsteadOfReturningZeros() throws Exception {
        FileReference file = fixture.writeAndHole(false, NON_CONTIGUOUS);
        try {
            readLargePage(file, new RowFileFixture.CloudCopyReadContext());
            Assert.fail("a large page whose extra pages cannot be read must not be returned");
        } catch (IllegalStateException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("cannot read a large page"));
        }
    }

    @Test
    public void aMissReadsThePagesAfterItInTheSameRequest() throws Exception {
        FileReference file = fixture.writeAndHole(false, NON_CONTIGUOUS);
        ioManager.cloudRequests.clear();
        // page 0, the large page's first slot, and the ordinary pages 2 to 4
        Assert.assertArrayEquals(ordinaryPage(0), fixture.readPage(file, readAhead(5 * SLOT), 0, 1));
        Assert.assertEquals(1, ioManager.cloudRequests.size());
        Assert.assertArrayEquals(new long[] { 0, 5 * SLOT }, ioManager.cloudRequests.get(0));

        ByteBuffer local = ByteBuffer.wrap(Files.readAllBytes(file.getFile().toPath()));
        Assert.assertEquals("a large page is left a hole", 0, local.getInt(LARGE_PAGE_ID * SLOT));
        ioManager.cloudRequests.clear();
        for (int pageId = CONTIGUOUS; pageId < NON_CONTIGUOUS; pageId++) {
            Assert.assertArrayEquals(ordinaryPage(pageId),
                    fixture.readPage(file, DefaultBufferCacheReadContextProvider.DEFAULT, pageId, 1));
        }
        Assert.assertArrayEquals(largePage(), readLargePage(file, cloudContext(true)));
        Assert.assertEquals("only the large page is read from the cloud", 2, ioManager.cloudRequests.size());
    }

    @Test
    public void aCompressedFileIsReadAheadOnlyWhereItsPagesFollowEachOther() throws Exception {
        // pages are stored in the order they are written: 0, 2, 3, 4, then the large page 1 and its extra pages
        FileReference file = fixture.writeAndHole(true, NON_CONTIGUOUS);
        ioManager.cloudRequests.clear();
        Assert.assertArrayEquals(ordinaryPage(0), fixture.readPage(file, readAhead(100 * SLOT), 0, 1));
        Assert.assertEquals("page 1 is not stored after page 0", 1, ioManager.cloudRequests.size());

        ioManager.cloudRequests.clear();
        Assert.assertArrayEquals(ordinaryPage(CONTIGUOUS),
                fixture.readPage(file, readAhead(100 * SLOT), CONTIGUOUS, 1));
        Assert.assertEquals(1, ioManager.cloudRequests.size());
        ioManager.cloudRequests.clear();
        for (int pageId = CONTIGUOUS + 1; pageId < NON_CONTIGUOUS; pageId++) {
            Assert.assertArrayEquals(ordinaryPage(pageId),
                    fixture.readPage(file, DefaultBufferCacheReadContextProvider.DEFAULT, pageId, 1));
        }
        Assert.assertTrue(ioManager.cloudRequests.isEmpty());
    }

    @Test
    public void aReadAheadStopsAtTheLastPage() throws Exception {
        FileReference file = fixture.writeAndHole(false, NON_CONTIGUOUS);
        int lastPageId = NON_CONTIGUOUS + FRAMES - 1;
        ioManager.cloudRequests.clear();
        Assert.assertArrayEquals(ordinaryPage(4), fixture.readPage(file, readAhead(100 * SLOT), 4, 1));
        Assert.assertArrayEquals(new long[] { 4 * SLOT, (lastPageId + 1 - 4) * SLOT }, ioManager.cloudRequests.get(0));
        Assert.assertArrayEquals(ordinaryPage(lastPageId),
                fixture.readPage(file, DefaultBufferCacheReadContextProvider.DEFAULT, lastPageId, 1));
    }

    @Test
    public void nothingIsReadAheadWhenTheDriveHasNoSpace() throws Exception {
        FileReference file = fixture.writeAndHole(false, NON_CONTIGUOUS);
        ioManager.cloudRequests.clear();
        ioManager.localWrites.clear();
        Assert.assertArrayEquals(ordinaryPage(0),
                fixture.readPage(file, new DefaultCloudReadContext(drive(false), 100 * SLOT), 0, 1));
        Assert.assertArrayEquals(new long[] { 0, SLOT }, ioManager.cloudRequests.get(0));
        Assert.assertTrue(ioManager.localWrites.isEmpty());
    }

    private static IBufferCacheReadContext readAhead(long size) {
        return new DefaultCloudReadContext(drive(true), size);
    }

    private byte[] readLargePage(FileReference file, IBufferCacheReadContext context) throws HyracksDataException {
        return fixture.readPage(file, context, LARGE_PAGE_ID, FRAMES);
    }

    private void assertReadWholeAndPersisted(boolean compressed, int extraBlockPageId) throws Exception {
        FileReference file = fixture.writeAndHole(compressed, extraBlockPageId);
        ioManager.localWrites.clear();
        ioManager.cloudRequests.clear();

        Assert.assertArrayEquals(largePage(), readLargePage(file, cloudContext(true)));
        Assert.assertEquals("the first page and the extra pages", 2, ioManager.cloudRequests.size());

        // the header, at the first page's offset, is written after everything else
        List<long[]> writes = ioManager.localWrites;
        long[] header = writes.get(writes.size() - 1);
        Assert.assertEquals(RESERVED_HEADER_BYTES, header[1]);
        for (int i = 0; i < writes.size() - 1; i++) {
            Assert.assertNotEquals("the header must be the last write", header[0], writes.get(i)[0]);
        }

        // the persisted copy is complete: reading it without any cloud fallback returns the whole page
        Assert.assertArrayEquals(largePage(), readLargePage(file, DefaultBufferCacheReadContextProvider.DEFAULT));
        if (!compressed) {
            Assert.assertEquals((long) LARGE_PAGE_ID * SLOT, header[0]);
        }
    }
}
