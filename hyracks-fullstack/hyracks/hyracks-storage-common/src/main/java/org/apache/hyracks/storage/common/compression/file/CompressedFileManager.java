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
package org.apache.hyracks.storage.common.compression.file;

import java.nio.ByteBuffer;
import java.util.EnumSet;

import org.apache.hyracks.api.compression.ICompressorDecompressor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.buffercache.ICachedPageInternal;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;

/**
 * CompressedFileManager is responsible to manage the Look Aside File (LAF file), which contains
 * the compression information. LAF file format is as follow:
 *
 * [<offset0, size0>, <offset1, size1> .... <offsetN, sizeN>]
 * Each entry <offsetM, sizeM> is an entry of 16-bytes for page M (8 bytes for offset and 8 for size).
 *
 * The file is responsible to store the beginning and the size of each page after compression.
 */
public class CompressedFileManager {
    protected static final int SIZE_ENTRY_OFFSET = 8; // 0 is for the compressed page offset
    protected static final int ENTRY_LENGTH = 16; //<offset(8-bytes),size(8-bytes)>
    protected static final int EOF = -1;
    private static final EnumSet<State> CLOSED = EnumSet.of(State.CLOSED);
    private static final EnumSet<State> CLOSED_OR_INVALID = EnumSet.of(State.CLOSED, State.INVALID);
    private static final EnumSet<State> OPEN = EnumSet.of(State.READABLE, State.WRITABLE);
    private static final EnumSet<State> OPEN_OR_INVALID = EnumSet.of(State.READABLE, State.WRITABLE, State.INVALID);
    private static final EnumSet<State> READABLE = EnumSet.of(State.READABLE);
    private static final EnumSet<State> WRITABLE = EnumSet.of(State.WRITABLE);

    private enum State {
        READABLE,
        WRITABLE,
        /*
         * INVALID state means that the LAF does not exists.
         * This could happened after deleting the LAF file but not the index file (e.g., non-graceful shutdown).
         * To account for this case, we should indicate the index is not valid and the index should be deleted by the
         * recovery manager.
         */
        INVALID,
        CLOSED
    }

    private final IBufferCache bufferCache;
    private final ICompressorDecompressor compressorDecompressor;
    private final CompressedFileReference fileRef;

    private int fileId;
    private State state;
    private int totalNumOfPages;

    private LAFWriter lafWriter;

    public CompressedFileManager(IBufferCache bufferCache, CompressedFileReference fileRef) {
        state = State.CLOSED;
        totalNumOfPages = 0;
        this.bufferCache = bufferCache;
        this.fileRef = fileRef;
        this.compressorDecompressor = fileRef.getCompressorDecompressor();
    }

    /* ************************
     * File handling methods
     * ************************
     */

    /**
     * Open LAF file
     * If the file is empty (i.e. the number of pages is zero)
     * Then the state will be WRITABLE.
     *
     * @return
     *         true if the file exists and was closed
     *         false otherwise
     * @throws IllegalStateException
     *             if the the file is not in CLOSED state
     */
    public boolean open() throws HyracksDataException {
        ensureState(CLOSED);

        boolean open = false;
        if (fileRef.getLAFFileReference().getFile().exists()) {
            fileId = bufferCache.openFile(fileRef.getLAFFileReference());
            open = true;
        } else {
            fileId = -1;
        }
        changeToFunctionalState();
        return open;
    }

    /**
     * Close LAF file
     *
     * @return
     *         true if the LAF file exists and was OPEN
     *         false otherwise
     * @throws IllegalStateException
     *             if the the file is not in OPEN state
     */
    public boolean close() throws HyracksDataException {
        ensureState(OPEN_OR_INVALID);

        boolean closed = false;
        if (state != State.INVALID) {
            bufferCache.closeFile(fileId);
            state = State.CLOSED;
            closed = true;
        }
        return closed;
    }

    /**
     * Close and purge LAF file
     *
     * @throws IllegalStateException
     *             if the the file is not in OPEN state
     */
    public void purge() throws HyracksDataException {
        if (close()) {
            bufferCache.purgeHandle(fileId);
        }
    }

    /**
     * Delete LAF file
     *
     * @throws IllegalStateException
     *             if the the file is not in CLOSED state
     */
    public void delete() throws HyracksDataException {
        ensureState(CLOSED_OR_INVALID);
        if (state != State.INVALID) {
            bufferCache.deleteFile(fileId);
        }
    }

    /**
     * Force LAF file content to disk
     *
     * @throws IllegalStateException
     *             if the the file is not in CLOSED state
     */
    public void force(boolean metadata) throws HyracksDataException {
        ensureState(OPEN);
        bufferCache.force(fileId, metadata);
    }

    /* ************************
     * LAF writing methods
     * ************************
     */

    /*
     * Should be only visible to LAFWriter
     */
    int getFileId() {
        return fileId;
    }

    public ICompressedPageWriter getCompressedPageWriter() {
        ensureState(WRITABLE);
        return lafWriter;
    }

    /**
     * Add page information (offset, size) after compression.
     *
     * @param dpid
     * @param size
     * @return offset for the compressed page.
     * @throws HyracksDataException
     */
    public long writePageInfo(long dpid, long size) throws HyracksDataException {
        final int pageId = BufferedFileHandle.getPageId(dpid);
        //Write the page (extraPageIndex = 0)
        return writeExtraPageInfo(pageId, size, 0);
    }

    /**
     * Add extra page information (offset, size) after compression.
     *
     * @param extraPageId
     *            extra page ID
     * @param size
     *            size of the extra page
     * @param extraPageIndex
     *            the index of the extra page (starting from 0)
     * @return offset for the compressed page.
     * @throws IllegalStateException
     *             If the file is not in WRITABLE state
     */
    public long writeExtraPageInfo(int extraPageId, long size, int extraPageIndex) throws HyracksDataException {
        ensureState(WRITABLE);

        final long compressedPageOffset;
        try {
            compressedPageOffset = lafWriter.writePageInfo(extraPageId + extraPageIndex, size);
        } catch (HyracksDataException e) {
            lafWriter.abort();
            throw e;
        }

        return compressedPageOffset;
    }

    /**
     * This methods is used by {@link LAFWriter#endWriting()} to signal the end of writing.
     * After calling this methods, LAF file will be READ-ONLY.
     *
     * @param totalNumOfPages
     *            The total number of pages of the index
     * @throws IllegalStateException
     *             If the file is not in WRITABLE state
     */
    void endWriting(int totalNumOfPages) {
        ensureState(WRITABLE);
        this.totalNumOfPages = totalNumOfPages;
        lafWriter = null;
        state = State.READABLE;
    }

    /* ************************
     * LAF reading methods
     * ************************
     */

    /**
     * Set the compressed page offset and size
     *
     * @param compressedPage
     */
    public void setCompressedPageInfo(ICachedPageInternal compressedPage) throws HyracksDataException {
        setCompressedPageInfo(BufferedFileHandle.getPageId(compressedPage.getDiskPageId()), compressedPage);
    }

    /**
     * Set the extra compressed page offset and size
     *
     * @param compressedPage
     * @param extraPageIndex
     */
    public void setExtraCompressedPageInfo(ICachedPageInternal compressedPage, int extraPageIndex)
            throws HyracksDataException {
        setCompressedPageInfo(compressedPage.getExtraBlockPageId() + extraPageIndex, compressedPage);
    }

    /* ************************
     * LAF general methods
     * ************************
     */

    /**
     * Get the number of compressed pages
     */
    public int getNumberOfPages() {
        return totalNumOfPages;
    }

    public ICompressorDecompressor getCompressorDecompressor() {
        return compressorDecompressor;
    }

    /* ************************
     * Private methods
     * ************************
     */

    private void ensureState(EnumSet<State> expectedStates) {
        if (!expectedStates.contains(state)) {
            throw new IllegalStateException(
                    "Expecting the state to be " + expectedStates + ". Currently it is " + state);
        }
    }

    private void changeToFunctionalState() throws HyracksDataException {
        if (fileId == -1) {
            state = State.INVALID;
        } else if (bufferCache.getNumPagesOfFile(fileId) == 0) {
            state = State.WRITABLE;
            lafWriter = new LAFWriter(this, bufferCache);
        } else {
            state = State.READABLE;
            init();
        }
    }

    private void init() throws HyracksDataException {
        final int numOfPages = bufferCache.getNumPagesOfFile(fileId);
        //Maximum number of entries in a page
        final int numOfEntriesPerPage = bufferCache.getPageSize() / ENTRY_LENGTH;
        //get the last page which may contain less entries than maxNumOfEntries
        final long dpid = getDiskPageId(numOfPages - 1);
        final ICachedPage page = bufferCache.pin(dpid, false);
        try {
            final ByteBuffer buf = page.getBuffer();

            //Start at 1 since it is impossible to have EOF at the first entry of a page
            int i = 1;
            //Seek EOF and count number of entries
            while (i < numOfEntriesPerPage && buf.getLong(i * ENTRY_LENGTH) != EOF) {
                i++;
            }

            totalNumOfPages = (numOfPages - 1) * numOfEntriesPerPage + i;
        } finally {
            bufferCache.unpin(page);
        }
    }

    private ICachedPage pinAndGetPage(int compressedPageId) throws HyracksDataException {
        final int pageId = compressedPageId * ENTRY_LENGTH / bufferCache.getPageSize();
        return bufferCache.pin(getDiskPageId(pageId), false);
    }

    private long getDiskPageId(int pageId) {
        return BufferedFileHandle.getDiskPageId(fileId, pageId);
    }

    private void setCompressedPageInfo(int compressedPageId, ICachedPageInternal compressedPage)
            throws HyracksDataException {
        ensureState(READABLE);
        if (totalNumOfPages == 0) {
            /*
             * It seems it is legal to pin empty file.
             * Return the page information as it is not compressed.
             */
            compressedPage.setCompressedPageOffset(0);
            compressedPage.setCompressedPageSize(bufferCache.getPageSize());
            return;
        }
        final ICachedPage page = pinAndGetPage(compressedPageId);
        try {
            // No need for read latches as pages are immutable.
            final ByteBuffer buf = page.getBuffer();
            final int entryOffset = compressedPageId * ENTRY_LENGTH % bufferCache.getPageSize();
            compressedPage.setCompressedPageOffset(buf.getLong(entryOffset));
            compressedPage.setCompressedPageSize((int) buf.getLong(entryOffset + SIZE_ENTRY_OFFSET));
        } finally {
            bufferCache.unpin(page);
        }
    }
}
