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
package org.apache.hyracks.cloud.test;

import static org.apache.hyracks.storage.common.buffercache.IBufferCache.RESERVED_HEADER_BYTES;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.cloud.buffercache.context.DefaultCloudReadContext;
import org.apache.hyracks.control.nc.io.IOManager;
import org.apache.hyracks.storage.common.buffercache.BufferCache;
import org.apache.hyracks.storage.common.buffercache.BufferCacheHeaderHelper;
import org.apache.hyracks.storage.common.buffercache.CachedPage;
import org.apache.hyracks.storage.common.buffercache.ClockPageReplacementStrategy;
import org.apache.hyracks.storage.common.buffercache.DefaultDiskCachedPageAllocator;
import org.apache.hyracks.storage.common.buffercache.DelayPageCleanerPolicy;
import org.apache.hyracks.storage.common.buffercache.HaltOnFailureCallback;
import org.apache.hyracks.storage.common.buffercache.HeapBufferAllocator;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.buffercache.IFIFOPageWriter;
import org.apache.hyracks.storage.common.buffercache.NoOpPageWriteCallback;
import org.apache.hyracks.storage.common.buffercache.context.IBufferCacheReadContext;
import org.apache.hyracks.storage.common.buffercache.context.read.DefaultBufferCacheReadContextProvider;
import org.apache.hyracks.storage.common.buffercache.context.write.DefaultBufferCacheWriteContext;
import org.apache.hyracks.storage.common.compression.SnappyCompressorDecompressor;
import org.apache.hyracks.storage.common.compression.file.CompressedFileReference;
import org.apache.hyracks.storage.common.compression.file.ICompressedPageWriter;
import org.apache.hyracks.storage.common.disk.IPhysicalDrive;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;
import org.apache.hyracks.storage.common.file.FileMapManager;
import org.apache.hyracks.util.IThreadStats;

/**
 * Writes files laid out as a B-tree's are, with ordinary pages and one large page, makes their bytes the cloud copy
 * in a {@link FakeCloudIOManager}, and reads their pages back through buffer caches of their own.
 */
public final class RowFileFixture {
    public static final int PAGE_SIZE = 256;
    public static final int SLOT = PAGE_SIZE + RESERVED_HEADER_BYTES;
    public static final int FRAMES = 3;
    public static final int LARGE_PAGE_ID = 1;

    private final FakeCloudIOManager ioManager;
    private final Map<BufferCache, Map<Integer, BufferedFileHandle>> fileInfoMaps = new HashMap<>();
    private int files;

    public RowFileFixture(FakeCloudIOManager ioManager) {
        this.ioManager = ioManager;
    }

    /**
     * Writes an ordinary page 0, then the pages a bulk loader would allocate between a large page and its extra
     * block, then the large page at {@link #LARGE_PAGE_ID}, then a last ordinary page at
     * {@code extraBlockPageId + FRAMES - 1}, and makes the file's bytes its cloud copy.
     */
    public FileReference write(boolean compressed, int extraBlockPageId) throws IOException {
        FileReference file = ioManager.resolve("component-" + compressed + "-" + extraBlockPageId + "-" + files++);
        if (compressed) {
            file = new CompressedFileReference(file.getDeviceHandle(), SnappyCompressorDecompressor.INSTANCE,
                    file.getRelativePath(), file.getRelativePath() + ".dic");
        }
        BufferCache bufferCache = createBufferCache(DefaultBufferCacheReadContextProvider.DEFAULT);
        try {
            int fileId = bufferCache.createFile(file);
            bufferCache.openFile(fileId);
            ICompressedPageWriter lafWriter = bufferCache.getCompressedPageWriter(fileId);
            IFIFOPageWriter writer = bufferCache.createFIFOWriter(NoOpPageWriteCallback.INSTANCE,
                    HaltOnFailureCallback.INSTANCE, DefaultBufferCacheWriteContext.INSTANCE);
            writeOrdinaryPage(bufferCache, lafWriter, writer, fileId, 0);
            for (int pageId = LARGE_PAGE_ID + 1; pageId < extraBlockPageId; pageId++) {
                writeOrdinaryPage(bufferCache, lafWriter, writer, fileId, pageId);
            }
            ICachedPage page = bufferCache.confiscateLargePage(BufferedFileHandle.getDiskPageId(fileId, LARGE_PAGE_ID),
                    FRAMES, extraBlockPageId);
            lafWriter.prepareWrite(page);
            page.getBuffer().put(0, largePage());
            writer.write(page);
            writeOrdinaryPage(bufferCache, lafWriter, writer, fileId, extraBlockPageId + FRAMES - 1);
            lafWriter.endWriting();
            bufferCache.closeFile(fileId);
        } finally {
            bufferCache.close();
        }
        ioManager.remote.put(file.getRelativePath(), Files.readAllBytes(file.getFile().toPath()));
        return file;
    }

    /**
     * As {@link #write}, then replaces the local file with a hole of the same length
     */
    public FileReference writeAndHole(boolean compressed, int extraBlockPageId) throws IOException {
        FileReference file = write(compressed, extraBlockPageId);
        try (RandomAccessFile raf = new RandomAccessFile(file.getFile(), "rw")) {
            long length = raf.length();
            raf.setLength(0);
            raf.setLength(length);
        }
        return file;
    }

    // A FileReference is registered with one buffer cache at a time, and each phase of a test uses a cache of its own
    public static FileReference fresh(FileReference file) {
        if (file instanceof CompressedFileReference) {
            return new CompressedFileReference(file.getDeviceHandle(), SnappyCompressorDecompressor.INSTANCE,
                    file.getRelativePath(), ((CompressedFileReference) file).getLAFRelativePath());
        }
        return new FileReference(file.getDeviceHandle(), file.getRelativePath());
    }

    /**
     * Reads a page through a buffer cache of its own
     */
    public byte[] readPage(FileReference file, IBufferCacheReadContext context, int pageId, int frames)
            throws HyracksDataException {
        BufferCache bufferCache = createBufferCache(context);
        try {
            int fileId = bufferCache.openFile(fresh(file));
            byte[] bytes = read(bufferCache, fileId, pageId, frames);
            bufferCache.closeFile(fileId);
            return bytes;
        } finally {
            bufferCache.close();
        }
    }

    public static byte[] read(BufferCache bufferCache, int fileId, int pageId, int frames) throws HyracksDataException {
        ICachedPage page = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, pageId));
        try {
            byte[] bytes = new byte[frames * PAGE_SIZE];
            page.getBuffer().get(0, bytes);
            return bytes;
        } finally {
            bufferCache.unpin(page);
        }
    }

    public BufferCache createBufferCache(IBufferCacheReadContext defaultContext) {
        HeapBufferAllocator allocator = new HeapBufferAllocator();
        ClockPageReplacementStrategy replacement =
                new ClockPageReplacementStrategy(allocator, DefaultDiskCachedPageAllocator.INSTANCE, PAGE_SIZE, 64);
        Map<Integer, BufferedFileHandle> fileInfoMap = new HashMap<>();
        BufferCache bufferCache = new BufferCache(ioManager, replacement, new DelayPageCleanerPolicy(1000),
                new FileMapManager(), 10, 10, r -> {
                    Thread thread = new Thread(r);
                    thread.setDaemon(true);
                    return thread;
                }, fileInfoMap, defaultContext);
        fileInfoMaps.put(bufferCache, fileInfoMap);
        return bufferCache;
    }

    public Map<Integer, BufferedFileHandle> fileInfoMap(BufferCache bufferCache) {
        return fileInfoMaps.get(bufferCache);
    }

    /**
     * Alternates a compressible and an incompressible frame, so that a compressed file stores some of the extra
     * pages compressed and some as they are.
     */
    public static byte[] largePage() {
        byte[] bytes = new byte[FRAMES * PAGE_SIZE];
        Random random = new Random(FRAMES);
        for (int frame = 0; frame < FRAMES; frame++) {
            byte[] frameBytes = new byte[PAGE_SIZE];
            if (frame % 2 == 0) {
                Arrays.fill(frameBytes, (byte) (frame + 1));
            } else {
                random.nextBytes(frameBytes);
            }
            System.arraycopy(frameBytes, 0, bytes, frame * PAGE_SIZE, PAGE_SIZE);
        }
        return bytes;
    }

    public static byte[] ordinaryPage(int pageId) {
        byte[] bytes = new byte[PAGE_SIZE];
        Arrays.fill(bytes, (byte) (100 + pageId));
        return bytes;
    }

    /**
     * @return a context that reads holes from the cloud copy one page at a time, saving them if {@code hasSpace}
     */
    public static IBufferCacheReadContext cloudContext(boolean hasSpace) {
        return new DefaultCloudReadContext(drive(hasSpace), 0L);
    }

    public static IPhysicalDrive drive(boolean hasSpace) {
        return new IPhysicalDrive() {
            @Override
            public boolean computeAndCheckIsPressured() {
                return !hasSpace;
            }

            @Override
            public boolean isUnpressured() {
                return hasSpace;
            }

            @Override
            public boolean hasSpace() {
                return hasSpace;
            }
        };
    }

    private static void writeOrdinaryPage(BufferCache bufferCache, ICompressedPageWriter lafWriter,
            IFIFOPageWriter writer, int fileId, int pageId) throws HyracksDataException {
        ICachedPage page = bufferCache.confiscatePage(BufferedFileHandle.getDiskPageId(fileId, pageId));
        lafWriter.prepareWrite(page);
        page.getBuffer().put(0, ordinaryPage(pageId));
        writer.write(page);
    }

    /**
     * Reads holes from the cloud copy and saves nothing, but does not override
     * {@link IBufferCacheReadContext#readHoleExtraPages}, so it cannot read a large page that is a hole
     */
    public static class CloudCopyReadContext implements IBufferCacheReadContext {
        @Override
        public void onPin(ICachedPage page) {
            // NoOp
        }

        @Override
        public void onUnpin(ICachedPage page) {
            // NoOp
        }

        @Override
        public boolean isNewPage() {
            return false;
        }

        @Override
        public boolean incrementStats() {
            return true;
        }

        @Override
        public ByteBuffer processHeader(IOManager ioManager, BufferedFileHandle fileHandle,
                BufferCacheHeaderHelper header, CachedPage cPage, IThreadStats threadStats)
                throws HyracksDataException {
            return DefaultCloudReadContext.readAndPersistPage(ioManager, fileHandle, header, cPage, threadStats, false);
        }
    }
}
