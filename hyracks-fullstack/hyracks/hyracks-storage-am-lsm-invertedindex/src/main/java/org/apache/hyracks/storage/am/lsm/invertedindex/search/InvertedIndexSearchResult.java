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

package org.apache.hyracks.storage.am.lsm.invertedindex.search;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.ListIterator;

import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.io.RunFileReader;
import org.apache.hyracks.dataflow.common.io.RunFileWriter;
import org.apache.hyracks.dataflow.std.buffermanager.BufferManagerBackedVSizeFrame;
import org.apache.hyracks.dataflow.std.buffermanager.ISimpleFrameBufferManager;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleWriter;
import org.apache.hyracks.storage.am.common.tuples.TypeAwareTupleWriter;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.IInvertedListSearchResultFrameTupleAppender;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.IInvertedListTupleReference;
import org.apache.hyracks.storage.am.lsm.invertedindex.ondisk.InvertedListSearchResultFrameTupleAppender;
import org.apache.hyracks.storage.am.lsm.invertedindex.util.InvertedIndexUtils;

/**
 * Disk-based or in-memory based storage for intermediate and final results of inverted-index
 * searches. One frame is dedicated to I/O operation for disk operation mode.
 */
public class InvertedIndexSearchResult {
    // The size of count field for each element. Currently, we use an integer value.
    protected static final int ELEMENT_COUNT_SIZE = 4;
    // I/O buffer's index in the buffers
    protected static final int IO_BUFFER_IDX = 0;
    protected static final String FILE_PREFIX = "InvertedIndexSearchResult";

    protected final IHyracksTaskContext ctx;
    protected final IInvertedListSearchResultFrameTupleAppender appender;
    protected final IFrameTupleAccessor accessor;
    protected final IInvertedListTupleReference tuple;
    protected final ISimpleFrameBufferManager bufferManager;
    protected ITypeTraits[] typeTraits;
    protected ITypeTraits[] invListFields;

    protected int currentWriterBufIdx;
    protected int currentReaderBufIdx;
    protected int numResults;
    // Read and Write I/O buffer
    protected IFrame ioBufferFrame = null;
    protected ByteBuffer ioBuffer = null;
    // Buffers for in-memory operation mode. The first buffer is the ioBuffer.
    // In case of the final search result, we will use only use the first buffer. No file will be created.
    protected ArrayList<ByteBuffer> buffers;

    protected RunFileWriter searchResultWriter;
    protected RunFileReader searchResultReader;
    protected boolean isInMemoryOpMode;
    protected boolean isInReadMode;
    protected boolean isWriteFinished;
    protected boolean isFileOpened;
    // Used for variable-size element in the inverted list
    protected ITreeIndexTupleWriter tupleWriter;
    protected byte[] tempBytes;

    public InvertedIndexSearchResult(ITypeTraits[] invListFields, IHyracksTaskContext ctx,
            ISimpleFrameBufferManager bufferManager) throws HyracksDataException {
        this.invListFields = invListFields;
        this.tupleWriter = new TypeAwareTupleWriter(invListFields);
        initTypeTraits(invListFields);
        this.ctx = ctx;
        appender = new InvertedListSearchResultFrameTupleAppender(ctx.getInitialFrameSize());
        accessor = InvertedIndexUtils.createInvertedListFrameTupleAccessor(ctx.getInitialFrameSize(), typeTraits);
        tuple = InvertedIndexUtils.createInvertedListTupleReference(typeTraits);
        this.bufferManager = bufferManager;
        this.isInReadMode = false;
        this.isWriteFinished = false;
        this.isInMemoryOpMode = false;
        this.isFileOpened = false;
        this.ioBufferFrame = null;
        this.ioBuffer = null;
        this.buffers = null;
        this.currentWriterBufIdx = 0;
        this.currentReaderBufIdx = 0;
        this.numResults = 0;
        this.tempBytes = new byte[ctx.getInitialFrameSize()];
        // Allocates one frame for read/write operation.
        prepareIOBuffer();
    }

    /**
     * Initializes the element type in the search result. In addition to the element, we will keep one more integer
     * per element to keep its occurrence count.
     */
    protected void initTypeTraits(ITypeTraits[] invListFields) {
        typeTraits = new ITypeTraits[invListFields.length + 1];
        for (int i = 0; i < invListFields.length; i++) {
            typeTraits[i] = invListFields[i];
        }
        // Integer for counting occurrences.
        typeTraits[invListFields.length] = IntegerPointable.TYPE_TRAITS;
    }

    // If all the inverted list fileds are fixed-size, then return the number of expected pages
    // Otherwise, return -1
    public int getExpectedNumPages(int numExpectedElements) {
        if (InvertedIndexUtils.checkTypeTraitsAllFixed(invListFields)) {
            int sizeElement = 0;
            for (int i = 0; i < invListFields.length; i++) {
                sizeElement += invListFields[i].getFixedLength();
            }

            int frameSize = ctx.getInitialFrameSize();
            // The count of Minframe, and the count of tuples in a frame should be deducted.
            frameSize = frameSize - InvertedListSearchResultFrameTupleAppender.MINFRAME_COUNT_SIZE
                    - InvertedListSearchResultFrameTupleAppender.TUPLE_COUNT_SIZE;
            int numPossibleElementPerPage = (int) Math.floor((double) frameSize / (sizeElement + ELEMENT_COUNT_SIZE));
            return (int) Math.ceil((double) numExpectedElements / numPossibleElementPerPage);
        } else {
            return -1;
        }
    }

    /**
     * Prepares the write operation. Tries to allocate buffers for the expected number of pages.
     * If that is possible, all operations will be executed in memory.
     * If not, all operations will use a file on disk except for the final search result.
     * A result of the final search result will be always in memory.
     */
    public void prepareWrite(int numExpectedPages) throws HyracksDataException {
        if (isInReadMode || isWriteFinished || searchResultWriter != null) {
            return;
        }
        // Intermediate results? disk or in-memory based
        // Allocates more buffers.
        if (InvertedIndexUtils.checkTypeTraitsAllFixed(typeTraits)) {
            isInMemoryOpMode = tryAllocateBuffers(numExpectedPages);
        } else {
            // When one of the type traits is variable length, disable the in memory mode
            // because the length of the inverted list is unknown, and thus may exceed the memory budget
            // A better way to do so might be to flush to disk when out-of-memory on the fly
            // instead of deciding the in memory mode or not before we merge the results
            isInMemoryOpMode = false;
        }
        if (!isInMemoryOpMode) {
            // Not enough number of buffers. Switch to the file I/O mode.
            createAndOpenFile();
        }
        appender.reset(ioBuffer);
        isWriteFinished = false;
    }

    protected int getNumBytesRequired(ITupleReference invListElement) {
        if (invListFields[0].isFixedLength()) {
            return invListElement.getFieldLength(0);
        } else {
            return tupleWriter.bytesRequired(invListElement, 0, 1);
        }
    }

    protected boolean appendInvertedListElement(ITupleReference invListElement) throws HyracksDataException {
        int numBytesRequired = getNumBytesRequired(invListElement);

        // Appends inverted-list element.
        if (invListFields[0].isFixedLength()) {
            if (!appender.append(invListElement.getFieldData(0), invListElement.getFieldStart(0),
                    invListElement.getFieldLength(0))) {
                throw HyracksDataException.create(ErrorCode.CANNOT_ADD_ELEMENT_TO_INVERTED_INDEX_SEARCH_RESULT);
            }
        } else {
            tupleWriter.writeTupleFields(invListElement, 0, 1, tempBytes, 0);
            if (!appender.append(tempBytes, 0, numBytesRequired)) {
                throw HyracksDataException.create(ErrorCode.CANNOT_ADD_ELEMENT_TO_INVERTED_INDEX_SEARCH_RESULT);
            }
        }

        return true;
    }

    /**
     * Appends an element and its count to the current frame of this result. The boolean value is necessary for
     * the final search result case since the append() of that class is **overriding** this method.
     *
     * Note that if the the buffer is run out, then this method will automatically write to the next buffer.
     * This is different from the append() method in the final search result which will simply return false.
     */
    public boolean append(ITupleReference invListElement, int count) throws HyracksDataException {

        int numBytesRequired = getNumBytesRequired(invListElement);
        ByteBuffer currentBuffer;
        // Moves to the next page if the current page is full.
        // + 4 for the count
        if (!appender.hasSpace(numBytesRequired + 4)) {
            currentWriterBufIdx++;
            if (isInMemoryOpMode) {
                currentBuffer = buffers.get(currentWriterBufIdx);
            } else {
                searchResultWriter.nextFrame(ioBuffer);
                currentBuffer = ioBuffer;
            }
            appender.reset(currentBuffer);
        }

        appendInvertedListElement(invListElement);

        // Appends count.
        if (!appender.append(count)) {
            throw HyracksDataException.create(ErrorCode.CANNOT_ADD_ELEMENT_TO_INVERTED_INDEX_SEARCH_RESULT);
        }
        appender.incrementTupleCount(1);
        numResults++;

        // Always true for the intermediate result. An append should not fail.
        return true;
    }

    /**
     * Finalizes the write operation. After this, no more write operation can be conducted.
     */
    public void finalizeWrite() throws HyracksDataException {
        if (isWriteFinished) {
            return;
        }
        // For in-memory operation (including the final result), no specific operations are required.
        // For disk-based operation, needs to close the writer.
        if (!isInMemoryOpMode && searchResultWriter != null) {
            searchResultWriter.nextFrame(ioBuffer);
            searchResultWriter.close();
        }
        isWriteFinished = true;
    }

    /**
     * Prepares a read operation.
     */
    public void prepareResultRead() throws HyracksDataException {
        if (isInReadMode) {
            return;
        }
        // No specific operation is required for in-memory mode (including the final result).
        if (!isInMemoryOpMode && searchResultWriter != null) {
            if (!isWriteFinished) {
                finalizeWrite();
            }
            searchResultReader = searchResultWriter.createDeleteOnCloseReader();
            searchResultReader.open();
            searchResultReader.setDeleteAfterClose(true);
        }
        currentReaderBufIdx = 0;
        isInReadMode = true;
    }

    /**
     * Gets the next frame of the current result file. A caller should make sure that initResultRead() is called first.
     */
    public ByteBuffer getNextFrame() throws HyracksDataException {
        ByteBuffer returnedBuffer = null;
        if (isInMemoryOpMode) {
            // In-memory mode for an intermediate search result
            returnedBuffer = buffers.get(currentReaderBufIdx);
            currentReaderBufIdx++;
        } else if (searchResultReader != null && searchResultReader.nextFrame(ioBufferFrame)) {
            // Disk-based mode for an intermediate search result
            returnedBuffer = ioBufferFrame.getBuffer();
        }
        return returnedBuffer;
    }

    /**
     * Finishes reading the result and frees the buffer.
     */
    public void closeResultRead(boolean deallocateIOBufferNeeded) throws HyracksDataException {
        if (isInMemoryOpMode) {
            // In-memory mode? Releases all buffers for an intermediate search result.
            deallocateBuffers();
        } else if (searchResultReader != null) {
            // Disk mode? Closes the file handle (this should delete the file also.)
            searchResultReader.close();
        }

        // Deallocates I/O buffer if requested.
        if (deallocateIOBufferNeeded) {
            deallocateIOBuffer();
        }
    }

    public int getCurrentBufferIndex() {
        return currentWriterBufIdx;
    }

    public ITypeTraits[] getTypeTraits() {
        return typeTraits;
    }

    public int getNumResults() {
        return numResults;
    }

    /**
     * Deletes any associated file and deallocates all buffers.
     */
    public void close() throws HyracksDataException {
        if (isInMemoryOpMode) {
            deallocateBuffers();
        } else {
            if (searchResultReader != null) {
                searchResultReader.close();
            } else if (searchResultWriter != null) {
                searchResultWriter.erase();
            }
        }
        deallocateIOBuffer();
    }

    public void reset() throws HyracksDataException {
        // Removes the file if it was in the disk op mode.
        if (searchResultReader != null) {
            searchResultReader.close();
        } else if (searchResultWriter != null) {
            searchResultWriter.erase();
        } else if (buffers.size() > 1) {
            // In-memory mode? Deallocates all buffers.
            deallocateBuffers();
        }

        // Resets the I/O buffer.
        clearBuffer(ioBuffer);

        searchResultWriter = null;
        searchResultReader = null;
        isInReadMode = false;
        isWriteFinished = false;
        isInMemoryOpMode = false;
        isFileOpened = false;
        currentWriterBufIdx = 0;
        currentReaderBufIdx = 0;
        numResults = 0;
    }

    /**
     * Allocates the buffer for read/write operation and initializes the buffers array that will be used keep a result.
     */
    protected void prepareIOBuffer() throws HyracksDataException {
        if (ioBufferFrame != null) {
            clearBuffer(ioBuffer);
        } else {
            ioBufferFrame = new BufferManagerBackedVSizeFrame(ctx, bufferManager);
            ioBuffer = ioBufferFrame.getBuffer();
            if (ioBuffer == null) {
                // One frame should be allocated for conducting read/write
                // operation. Otherwise, can't store the result.
                throw HyracksDataException.create(ErrorCode.NOT_ENOUGH_BUDGET_FOR_TEXTSEARCH,
                        this.getClass().getSimpleName());
            }
            clearBuffer(ioBuffer);
            // For keeping the results in memory if possible.
            buffers = new ArrayList<ByteBuffer>();
            buffers.add(ioBuffer);
        }
    }

    /**
     * Tries to allocate buffers to accommodate the results in memory.
     */
    protected boolean tryAllocateBuffers(int numExpectedPages) throws HyracksDataException {
        assert numExpectedPages >= 0;

        boolean allBufferAllocated = true;
        while (buffers.size() < numExpectedPages) {
            // Currently, the buffers (optional, needs multiple pages, for in-memory mode)
            // and the ioBuffer (must-have, needs one page only, for disk IO, related code is in the above prepareIOBuffer())
            // are both acquired from the same bufferManager.
            // It may be possible that one search result acquires all the frame for its in-memory usage,
            // and then the next search result cannot even get one frame for its disk IO usage.
            // In this case, the second search result will exit with an out-of-memory error.
            //
            // To avoid the above issue, maybe we need to create **two** buffer managers, one to manage in-memory frame usage,
            // and the other to manage on-disk frame usage to guarantee that every search result has at least one disk IO frame.
            // Or, to make things simpler, we can let the search result manage its own disk IO frame,
            // i.e. create the frame on its own rather than acquire from the buffer manager.
            // In this case, we cannot reuse frames, but each search result needs only one IO frame,
            // and the number of search result is pretty limited (e.g. one result per query keyword).
            ByteBuffer tmpBuffer = bufferManager.acquireFrame(ctx.getInitialFrameSize());
            if (tmpBuffer == null) {
                // Budget exhausted
                allBufferAllocated = false;
                deallocateBuffers();
                break;
            } else {
                clearBuffer(tmpBuffer);
            }
            buffers.add(tmpBuffer);
        }
        return allBufferAllocated;
    }

    // Creates a file for the writer.
    protected void createAndOpenFile() throws HyracksDataException {
        if (isInMemoryOpMode) {
            // In-memory mode should not generate a file.
            return;
        }
        if (searchResultWriter == null) {
            FileReference file = ctx.getJobletContext().createManagedWorkspaceFile(FILE_PREFIX);
            searchResultWriter = new RunFileWriter(file, ctx.getIoManager());
            searchResultWriter.open();
            isFileOpened = true;
        }
    }

    // Deallocates the I/O buffer (one frame). This should be the last oepration.
    protected void deallocateIOBuffer() throws HyracksDataException {
        if (ioBufferFrame != null) {
            bufferManager.releaseFrame(ioBuffer);
            buffers.clear();
            ioBufferFrame = null;
            ioBuffer = null;
        }
    }

    /**
     * Deallocates the buffers. We do not remove the first buffer since it can be used as an I/O buffer.
     */
    protected void deallocateBuffers() throws HyracksDataException {
        int toDeleteCount = buffers.size() - 1;
        int deletedCount = 0;
        for (ListIterator<ByteBuffer> iter = buffers.listIterator(buffers.size()); iter.hasPrevious();) {
            if (deletedCount >= toDeleteCount) {
                break;
            }
            ByteBuffer next = iter.previous();
            bufferManager.releaseFrame(next);
            iter.remove();
            deletedCount++;
        }
    }

    public IFrameTupleAccessor getAccessor() {
        return accessor;
    }

    public IInvertedListSearchResultFrameTupleAppender getAppender() {
        return appender;
    }

    public IInvertedListTupleReference getTuple() {
        return tuple;
    }

    protected void clearBuffer(ByteBuffer bufferToClear) {
        Arrays.fill(bufferToClear.array(), (byte) 0);
        bufferToClear.clear();
    }

    protected void resetAppenderLocation(int bufferIdx) {
        accessor.reset(buffers.get(bufferIdx));
        appender.reset(buffers.get(bufferIdx), false, accessor.getTupleCount(),
                accessor.getTupleEndOffset(accessor.getTupleCount() - 1));
    }

}
