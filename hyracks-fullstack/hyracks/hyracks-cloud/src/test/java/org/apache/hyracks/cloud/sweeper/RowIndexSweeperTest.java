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
package org.apache.hyracks.cloud.sweeper;

import static org.apache.hyracks.cloud.test.RowFileFixture.FRAMES;
import static org.apache.hyracks.cloud.test.RowFileFixture.LARGE_PAGE_ID;
import static org.apache.hyracks.cloud.test.RowFileFixture.cloudContext;
import static org.apache.hyracks.cloud.test.RowFileFixture.fresh;
import static org.apache.hyracks.cloud.test.RowFileFixture.largePage;
import static org.apache.hyracks.cloud.test.RowFileFixture.ordinaryPage;
import static org.apache.hyracks.cloud.test.RowFileFixture.read;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IODeviceHandle;
import org.apache.hyracks.cloud.test.FakeCloudIOManager;
import org.apache.hyracks.cloud.test.RowFileFixture;
import org.apache.hyracks.control.nc.io.IOManager;
import org.apache.hyracks.storage.common.buffercache.BufferCache;
import org.apache.hyracks.storage.common.buffercache.BufferCacheHeaderHelper;
import org.apache.hyracks.storage.common.buffercache.CachedPage;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;
import org.apache.hyracks.util.IThreadStats;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Punching a row-based index's file must turn every page of it, including those in the buffer cache and the extra
 * slots of a large page, into a hole that reads back whole from the cloud storage. It must not start while a page of
 * the file is being loaded, and loads that arrive meanwhile must wait for it.
 */
public class RowIndexSweeperTest {
    // the bulk loader allocates the ordinary pages 2 to 4 between the large page and its extra block
    private static final int EXTRA_BLOCK_PAGE_ID = 5;
    private static final int LAST_PAGE_ID = EXTRA_BLOCK_PAGE_ID + FRAMES - 1;
    private static final int RESIDENT_ORDINARY_PAGE_ID = 3;

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
    public void everyPageIsPunchedAndReadsBackWholeFromTheCloud() throws Exception {
        for (boolean compressed : new boolean[] { false, true }) {
            FileReference file = fixture.write(compressed, EXTRA_BLOCK_PAGE_ID);
            long size = Files.size(file.getFile().toPath());
            ioManager.punches.clear();
            // the pages in the buffer cache are punched too
            sweep(file, LARGE_PAGE_ID, RESIDENT_ORDINARY_PAGE_ID);
            Assert.assertEquals("one punch over the whole file", 1, ioManager.punches.size());
            Assert.assertArrayEquals(new long[] { 0, size }, ioManager.punches.get(0));
            Assert.assertArrayEquals(new byte[Math.toIntExact(size)], Files.readAllBytes(file.getFile().toPath()));

            ioManager.cloudRequests.clear();
            assertAllPagesRead(file);
            int ordinaryPages = EXTRA_BLOCK_PAGE_ID - LARGE_PAGE_ID + 1;
            Assert.assertEquals("every page, the large one in two reads", ordinaryPages + 2,
                    ioManager.cloudRequests.size());
        }
    }

    @Test
    public void anIdleFileIsPunchedAfterItsDescriptorIsReleased() throws Exception {
        FileReference file = fixture.write(false, EXTRA_BLOCK_PAGE_ID);
        long size = Files.size(file.getFile().toPath());
        BufferCache bufferCache = fixture.createBufferCache(cloudContext(true));
        RowIndexSweeper sweeper = new RowIndexSweeper(ioManager, fixture.fileInfoMap(bufferCache));
        try {
            int fileId = bufferCache.openFile(fresh(file));
            BufferedFileHandle handle = fixture.fileInfoMap(bufferCache).get(fileId);
            Assert.assertTrue(handle.tryReleaseDescriptor());
            ioManager.punches.clear();
            Assert.assertTrue(sweeper.sweepFile(handle));
            Assert.assertArrayEquals(new long[] { 0, size }, ioManager.punches.get(0));
            bufferCache.closeFile(fileId);
        } finally {
            bufferCache.close();
        }
        Assert.assertArrayEquals(new byte[Math.toIntExact(size)], Files.readAllBytes(file.getFile().toPath()));
    }

    @Test
    public void aFileClosedMeanwhileIsLeftAsItIs() throws Exception {
        FileReference file = fixture.write(false, EXTRA_BLOCK_PAGE_ID);
        byte[] original = Files.readAllBytes(file.getFile().toPath());
        BufferCache bufferCache = fixture.createBufferCache(cloudContext(true));
        RowIndexSweeper sweeper = new RowIndexSweeper(ioManager, fixture.fileInfoMap(bufferCache));
        try {
            int fileId = bufferCache.openFile(fresh(file));
            BufferedFileHandle handle = fixture.fileInfoMap(bufferCache).get(fileId);
            bufferCache.closeFile(fileId);
            // as when its index is deactivated while the sweeper holds one of its components
            bufferCache.purgeHandle(fileId);
            Assert.assertFalse(sweeper.sweepFile(handle));
            Assert.assertFalse("the file must not be reopened", fixture.fileInfoMap(bufferCache).containsKey(fileId));
            Assert.assertFalse(handle.tryPunch(fh -> {
                throw new AssertionError("the handle is closed");
            }));
        } finally {
            bufferCache.close();
        }
        Assert.assertTrue(ioManager.punches.isEmpty());
        Assert.assertArrayEquals(original, Files.readAllBytes(file.getFile().toPath()));
    }

    @Test
    public void aLoadDuringAPunchWaitsForIt() throws Exception {
        FileReference file = fixture.write(false, EXTRA_BLOCK_PAGE_ID);
        BufferCache bufferCache = fixture.createBufferCache(cloudContext(true));
        try {
            int fileId = bufferCache.openFile(fresh(file));
            BufferedFileHandle handle = fixture.fileInfoMap(bufferCache).get(fileId);
            long size = handle.getFileSize();
            FutureTask<byte[]> load = new FutureTask<>(() -> read(bufferCache, fileId, 0, 1));
            ioManager.cloudRequests.clear();
            Assert.assertTrue(handle.tryPunch(fh -> {
                ioManager.punchHole(fh, 0L, size);
                Thread loader = new Thread(load);
                loader.start();
                Assert.assertTrue("a load must wait for the punch", waitsForTheLoadLock(loader, load));
                return null;
            }));
            Assert.assertArrayEquals(ordinaryPage(0), get(load));
            Assert.assertEquals("the page is a hole by the time it is loaded", 1, ioManager.cloudRequests.size());
            bufferCache.closeFile(fileId);
        } finally {
            bufferCache.close();
        }
    }

    @Test
    public void aPunchDoesNotStartWhileAPageIsBeingLoaded() throws Exception {
        FileReference file = fixture.write(false, EXTRA_BLOCK_PAGE_ID);
        CountDownLatch loading = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        BufferCache bufferCache = fixture.createBufferCache(new BlockingReadContext(loading, release));
        try {
            int fileId = bufferCache.openFile(fresh(file));
            BufferedFileHandle handle = fixture.fileInfoMap(bufferCache).get(fileId);
            FutureTask<byte[]> load = new FutureTask<>(() -> read(bufferCache, fileId, 0, 1));
            new Thread(load).start();
            Assert.assertTrue(loading.await(10, TimeUnit.SECONDS));
            Assert.assertFalse("a page of the file is being loaded", handle.tryPunch(fh -> {
                throw new AssertionError("the punch must not run");
            }));
            release.countDown();
            Assert.assertArrayEquals(ordinaryPage(0), get(load));
            Assert.assertTrue(handle.tryPunch(fh -> null));
            bufferCache.closeFile(fileId);
        } finally {
            release.countDown();
            bufferCache.close();
        }
    }

    /**
     * Makes the given pages resident, then punches the file
     */
    private void sweep(FileReference file, int... residentPageIds) throws HyracksDataException {
        BufferCache bufferCache = fixture.createBufferCache(cloudContext(true));
        RowIndexSweeper sweeper = new RowIndexSweeper(ioManager, fixture.fileInfoMap(bufferCache));
        try {
            int fileId = bufferCache.openFile(fresh(file));
            for (int pageId : residentPageIds) {
                read(bufferCache, fileId, pageId, pageId == LARGE_PAGE_ID ? FRAMES : 1);
            }
            Assert.assertTrue(sweeper.sweepFile(fixture.fileInfoMap(bufferCache).get(fileId)));
            bufferCache.closeFile(fileId);
        } finally {
            bufferCache.close();
        }
    }

    // each through a cache of its own, which saves nothing
    private void assertAllPagesRead(FileReference file) throws HyracksDataException {
        Assert.assertArrayEquals(largePage(), fixture.readPage(file, cloudContext(false), LARGE_PAGE_ID, FRAMES));
        for (int pageId = 0; pageId <= LAST_PAGE_ID; pageId++) {
            // the large page's extra pages are read with it
            if (pageId == LARGE_PAGE_ID || (pageId >= EXTRA_BLOCK_PAGE_ID && pageId < LAST_PAGE_ID)) {
                continue;
            }
            Assert.assertArrayEquals("page " + pageId, ordinaryPage(pageId),
                    fixture.readPage(file, cloudContext(false), pageId, 1));
        }
    }

    private static <T> T get(Future<T> future) throws Exception {
        return future.get(10, TimeUnit.SECONDS);
    }

    /**
     * @return whether {@code thread} came to wait on a read-write lock, which in a load can only be the file's load
     *         lock, before {@code task} finished
     */
    private static boolean waitsForTheLoadLock(Thread thread, Future<?> task) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!task.isDone() && System.nanoTime() - deadline < 0) {
            Object blocker = LockSupport.getBlocker(thread);
            if (blocker != null && blocker.getClass().getName().startsWith(ReentrantReadWriteLock.class.getName())) {
                return true;
            }
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
        }
        return false;
    }

    /**
     * Signals {@code loading} and waits for {@code release} before reading a page
     */
    private static final class BlockingReadContext extends RowFileFixture.CloudCopyReadContext {
        private final CountDownLatch loading;
        private final CountDownLatch release;

        private BlockingReadContext(CountDownLatch loading, CountDownLatch release) {
            this.loading = loading;
            this.release = release;
        }

        @Override
        public ByteBuffer processHeader(IOManager ioManager, BufferedFileHandle fileHandle,
                BufferCacheHeaderHelper header, CachedPage cPage, IThreadStats threadStats)
                throws HyracksDataException {
            loading.countDown();
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw HyracksDataException.create(e);
            }
            return super.processHeader(ioManager, fileHandle, header, cPage, threadStats);
        }
    }
}
