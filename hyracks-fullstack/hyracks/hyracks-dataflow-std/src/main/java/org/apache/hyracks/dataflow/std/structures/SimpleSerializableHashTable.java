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
package org.apache.hyracks.dataflow.std.structures;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hyracks.api.context.IHyracksFrameMgrContext;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionComputer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.std.buffermanager.ITuplePointerAccessor;

/**
 * This table consists of header frames and content frames.
 * Header indicates the first entry slot location for the given integer value.
 * A header slot consists of [content frame number], [offset in that frame] to get
 * the first tuple's pointer information that shares the same hash value.
 * An entry slot in the content frame is as follows.
 * [capacity of the slot], [# of occupied elements], {[frameIndex], [tupleIndex]}+;
 * <fIndex, tIndex> forms a tuple pointer.
 * WARNING: this hash table can grow up indefinitely and may generate Out Of Memory Exception.
 * So, do not use this in production and use SerializableHashTable class instead
 * since that should be managed by a buffer manager.
 */
public class SimpleSerializableHashTable implements ISerializableTable {

    // unit size: int
    protected static final int INT_SIZE = 4;
    // Initial entry slot size
    protected static final int INIT_ENTRY_SIZE = 4;
    protected static final int INVALID_VALUE = 0xFFFFFFFF;
    protected static final byte INVALID_BYTE_VALUE = (byte) 0xFF;

    // Header frame array
    protected IntSerDeBuffer[] headers;
    // Content frame list
    protected List<IntSerDeBuffer> contents = new ArrayList<>();
    protected List<Integer> currentOffsetInEachFrameList = new ArrayList<>();
    protected int frameCapacity;
    protected int currentLargestFrameNumber = 0;
    // The byte size of total frames that are allocated to the headers and contents
    protected int currentByteSize = 0;
    protected int tupleCount = 0;
    protected TuplePointer tempTuplePointer = new TuplePointer();
    protected int tableSize;
    protected int frameSize;
    protected IHyracksFrameMgrContext ctx;

    public SimpleSerializableHashTable(int tableSize, final IHyracksFrameMgrContext ctx) throws HyracksDataException {
        this(tableSize, ctx, true);
    }

    public SimpleSerializableHashTable(int tableSize, final IHyracksFrameMgrContext ctx, boolean frameInitRequired)
            throws HyracksDataException {
        this.ctx = ctx;
        frameSize = ctx.getInitialFrameSize();
        int residual = tableSize * INT_SIZE * 2 % frameSize == 0 ? 0 : 1;
        int headerSize = tableSize * INT_SIZE * 2 / frameSize + residual;
        headers = new IntSerDeBuffer[headerSize];
        this.tableSize = tableSize;
        if (frameInitRequired) {
            ByteBuffer newFrame = getFrame(frameSize);
            if (newFrame == null) {
                throw new HyracksDataException("Can't initialize the Hash Table. Please assign more memory.");
            }
            IntSerDeBuffer frame = new IntSerDeBuffer(newFrame);
            frameCapacity = frame.capacity();
            contents.add(frame);
            currentOffsetInEachFrameList.add(0);
        }
    }

    ByteBuffer getFrame(int size) throws HyracksDataException {
        currentByteSize += size;
        return ctx.allocateFrame(size);
    }

    void increaseWastedSpaceCount(int size) {
        // Do nothing. For this simple implementation, we don't count the wasted space.
    }

    @Override
    public boolean insert(int entry, TuplePointer pointer) throws HyracksDataException {
        int headerFrameIndex = getHeaderFrameIndex(entry);
        int offsetInHeaderFrame = getHeaderFrameOffset(entry);
        IntSerDeBuffer headerFrame = headers[headerFrameIndex];
        if (headerFrame == null) {
            ByteBuffer newFrame = getFrame(frameSize);
            if (newFrame == null) {
                return false;
            }
            headerFrame = new IntSerDeBuffer(newFrame);
            headers[headerFrameIndex] = headerFrame;
        }
        int contentFrameIndex = headerFrame.getInt(offsetInHeaderFrame);
        boolean result;
        if (contentFrameIndex < 0) {
            // Since the initial value of index and offset is -1, this means that the slot for
            // this entry is not created yet. So, create the entry slot and insert first tuple into that slot.
            // OR, the previous slot becomes full and the newly double-sized slot is about to be created.
            result = insertNewEntry(headerFrame, offsetInHeaderFrame, INIT_ENTRY_SIZE, pointer);
        } else {
            // The entry slot already exists. Insert non-first tuple into the entry slot
            int offsetInContentFrame = headerFrame.getInt(offsetInHeaderFrame + 1);
            result = insertNonFirstTuple(headerFrame, offsetInHeaderFrame, contentFrameIndex, offsetInContentFrame,
                    pointer);
        }

        if (result) {
            tupleCount++;
        }

        return result;
    }

    @Override
    /**
     * Reset the slot information for the entry. The connection (pointer) between header frame and
     * content frame will be also lost. Specifically, we reset the number of used count in the slot as -1
     * so that the space could be reclaimed.
     */
    public void delete(int entry) {
        int headerFrameIndex = getHeaderFrameIndex(entry);
        int offsetInHeaderFrame = getHeaderFrameOffset(entry);
        IntSerDeBuffer header = headers[headerFrameIndex];
        if (header != null) {
            int contentFrameIndex = header.getInt(offsetInHeaderFrame);
            int offsetInContentFrame = header.getInt(offsetInHeaderFrame + 1);
            if (contentFrameIndex >= 0) {
                IntSerDeBuffer frame = contents.get(contentFrameIndex);
                int entrySlotCapacity = frame.getInt(offsetInContentFrame);
                int entryUsedItems = frame.getInt(offsetInContentFrame + 1);
                // Set used count as -1 in the slot so that the slot space could be reclaimed.
                frame.writeInvalidVal(offsetInContentFrame + 1, 1);
                // Also reset the header (frmaeIdx, offset) to content frame pointer.
                header.writeInvalidVal(offsetInHeaderFrame, 2);
                tupleCount = tupleCount - entryUsedItems;
                increaseWastedSpaceCount((entrySlotCapacity + 1) * 2);
            }
        }
    }

    @Override
    /**
     * For the given integer value, get the n-th (n = offsetInSlot) tuple pointer in the corresponding slot.
     */
    public boolean getTuplePointer(int entry, int offsetInSlot, TuplePointer dataPointer) {
        int headerFrameIndex = getHeaderFrameIndex(entry);
        int offsetInHeaderFrame = getHeaderFrameOffset(entry);
        IntSerDeBuffer header = headers[headerFrameIndex];
        if (header == null) {
            dataPointer.reset(INVALID_VALUE, INVALID_VALUE);
            return false;
        }
        int contentFrameIndex = header.getInt(offsetInHeaderFrame);
        int offsetInContentFrame = header.getInt(offsetInHeaderFrame + 1);
        if (contentFrameIndex < 0) {
            dataPointer.reset(INVALID_VALUE, INVALID_VALUE);
            return false;
        }
        IntSerDeBuffer frame = contents.get(contentFrameIndex);
        int entryUsedCountInSlot = frame.getInt(offsetInContentFrame + 1);
        if (offsetInSlot > entryUsedCountInSlot - 1) {
            dataPointer.reset(INVALID_VALUE, INVALID_VALUE);
            return false;
        }
        int startOffsetInContentFrame = offsetInContentFrame + 2 + offsetInSlot * 2;
        while (startOffsetInContentFrame >= frameCapacity) {
            ++contentFrameIndex;
            startOffsetInContentFrame -= frameCapacity;
        }
        frame = contents.get(contentFrameIndex);
        dataPointer.reset(frame.getInt(startOffsetInContentFrame), frame.getInt(startOffsetInContentFrame + 1));
        return true;
    }

    @Override
    public void reset() {
        for (IntSerDeBuffer frame : headers) {
            if (frame != null) {
                frame.resetFrame();
            }
        }

        currentOffsetInEachFrameList.clear();
        for (int i = 0; i < contents.size(); i++) {
            currentOffsetInEachFrameList.add(0);
        }

        currentLargestFrameNumber = 0;
        tupleCount = 0;
        currentByteSize = 0;
    }

    @Override
    public int getCurrentByteSize() {
        return currentByteSize;
    }

    @Override
    public int getTupleCount() {
        return tupleCount;
    }

    @Override
    /**
     * Returns the tuple count in the slot for the given entry.
     */
    public int getTupleCount(int entry) {
        int headerFrameIndex = getHeaderFrameIndex(entry);
        int offsetInHeaderFrame = getHeaderFrameOffset(entry);
        IntSerDeBuffer headerFrame = headers[headerFrameIndex];
        if (headerFrame != null) {
            int contentFrameIndex = headerFrame.getInt(offsetInHeaderFrame);
            int offsetInContentFrame = headerFrame.getInt(offsetInHeaderFrame + 1);
            if (contentFrameIndex >= 0) {
                IntSerDeBuffer frame = contents.get(contentFrameIndex);
                int entryUsedCountInSlot = frame.getInt(offsetInContentFrame + 1);
                return entryUsedCountInSlot;
            }
        }
        return 0;
    }

    @Override
    public void close() {
        int nFrames = contents.size();
        for (int i = 0; i < headers.length; i++) {
            headers[i] = null;
        }
        contents.clear();
        currentOffsetInEachFrameList.clear();
        tupleCount = 0;
        currentByteSize = 0;
        currentLargestFrameNumber = 0;
        ctx.deallocateFrames(nFrames);
    }

    protected boolean insertNewEntry(IntSerDeBuffer header, int offsetInHeaderFrame, int entryCapacity,
            TuplePointer pointer) throws HyracksDataException {
        IntSerDeBuffer lastContentFrame = contents.get(currentLargestFrameNumber);
        int lastOffsetInCurrentFrame = currentOffsetInEachFrameList.get(currentLargestFrameNumber);
        int requiredIntCapacity = entryCapacity * 2;
        int currentFrameNumber = currentLargestFrameNumber;
        boolean currentFrameNumberChanged = false;

        if (lastOffsetInCurrentFrame + requiredIntCapacity >= frameCapacity) {
            IntSerDeBuffer newContentFrame;
            // At least we need to have the mata-data (slot capacity and used count) and
            // one tuplePointer in the same frame (4 INT_SIZE).
            // So, if there is not enough space for this, we just move on to the next page.
            if ((lastOffsetInCurrentFrame + 4) > frameCapacity) {
                // Swipe the region that can't be used.
                lastContentFrame.writeInvalidVal(lastOffsetInCurrentFrame, frameCapacity - lastOffsetInCurrentFrame);
                currentFrameNumber++;
                lastOffsetInCurrentFrame = 0;
                currentFrameNumberChanged = true;
            }
            do {
                if (currentLargestFrameNumber >= contents.size() - 1) {
                    ByteBuffer newFrame = getFrame(frameSize);
                    if (newFrame == null) {
                        return false;
                    }
                    newContentFrame = new IntSerDeBuffer(newFrame);
                    currentLargestFrameNumber++;
                    contents.add(newContentFrame);
                    currentOffsetInEachFrameList.add(0);
                } else {
                    currentLargestFrameNumber++;
                    currentOffsetInEachFrameList.set(currentLargestFrameNumber, 0);
                }
                requiredIntCapacity -= frameCapacity;
            } while (requiredIntCapacity > 0);
        }

        if (currentFrameNumberChanged) {
            lastContentFrame = contents.get(currentFrameNumber);
        }

        // sets the header
        header.writeInt(offsetInHeaderFrame, currentFrameNumber);
        header.writeInt(offsetInHeaderFrame + 1, lastOffsetInCurrentFrame);

        // sets the entry & its slot.
        // 1. slot capacity
        lastContentFrame.writeInt(lastOffsetInCurrentFrame, entryCapacity - 1);
        // 2. used count in the slot
        lastContentFrame.writeInt(lastOffsetInCurrentFrame + 1, 1);
        // 3. initial entry in the slot
        lastContentFrame.writeInt(lastOffsetInCurrentFrame + 2, pointer.getFrameIndex());
        lastContentFrame.writeInt(lastOffsetInCurrentFrame + 3, pointer.getTupleIndex());
        int newLastOffsetInContentFrame = lastOffsetInCurrentFrame + entryCapacity * 2;
        newLastOffsetInContentFrame =
                newLastOffsetInContentFrame < frameCapacity ? newLastOffsetInContentFrame : frameCapacity - 1;
        currentOffsetInEachFrameList.set(currentFrameNumber, newLastOffsetInContentFrame);

        requiredIntCapacity = entryCapacity * 2 - (frameCapacity - lastOffsetInCurrentFrame);
        while (requiredIntCapacity > 0) {
            currentFrameNumber++;
            requiredIntCapacity -= frameCapacity;
            newLastOffsetInContentFrame =
                    requiredIntCapacity < 0 ? requiredIntCapacity + frameCapacity : frameCapacity - 1;
            currentOffsetInEachFrameList.set(currentFrameNumber, newLastOffsetInContentFrame);
        }

        return true;
    }

    protected boolean insertNonFirstTuple(IntSerDeBuffer header, int offsetInHeaderFrame, int contentFrameIndex,
            int offsetInContentFrame, TuplePointer pointer) throws HyracksDataException {
        int frameIndex = contentFrameIndex;
        IntSerDeBuffer contentFrame = contents.get(frameIndex);
        int entrySlotCapacity = contentFrame.getInt(offsetInContentFrame);
        int entryUsedCountInSlot = contentFrame.getInt(offsetInContentFrame + 1);
        boolean frameIndexChanged = false;
        if (entryUsedCountInSlot < entrySlotCapacity) {
            // The slot has at least one space to accommodate this tuple pointer.
            // Increase the used count by 1.
            contentFrame.writeInt(offsetInContentFrame + 1, entryUsedCountInSlot + 1);
            // Calculates the first empty spot in the slot.
            // +2: (capacity, # of used entry count)
            // *2: each tuplePointer's occupation (frame index + offset in that frame)
            int startOffsetInContentFrame = offsetInContentFrame + 2 + entryUsedCountInSlot * 2;
            while (startOffsetInContentFrame >= frameCapacity) {
                ++frameIndex;
                startOffsetInContentFrame -= frameCapacity;
                frameIndexChanged = true;
            }
            // We don't have to read content frame again if the frame index has not been changed.
            if (frameIndexChanged) {
                contentFrame = contents.get(frameIndex);
            }
            contentFrame.writeInt(startOffsetInContentFrame, pointer.getFrameIndex());
            contentFrame.writeInt(startOffsetInContentFrame + 1, pointer.getTupleIndex());
        } else {
            // There is no enough space in this slot. We need to increase the slot size and
            // migrate the current entries in it.

            // New capacity: double the original capacity
            int capacity = (entrySlotCapacity + 1) * 2;

            // Temporarily sets the header (frameIdx, offset) as (-1,-1) for the slot.
            header.writeInvalidVal(offsetInHeaderFrame, 2);
            // Marks the old slot as obsolete - set the used count as -1 so that its space can be reclaimed
            // when a garbage collection is executed.
            contentFrame.writeInvalidVal(offsetInContentFrame + 1, 1);

            // Gets the location of the initial entry.
            int fIndex = contentFrame.getInt(offsetInContentFrame + 2);
            int tIndex = contentFrame.getInt(offsetInContentFrame + 3);
            tempTuplePointer.reset(fIndex, tIndex);
            // Creates a new double-sized slot for the current entries and
            // migrates the initial entry in the slot to the new slot.
            if (!this.insertNewEntry(header, offsetInHeaderFrame, capacity, tempTuplePointer)) {
                // Reverses the effect of change.
                header.writeInt(offsetInHeaderFrame, contentFrameIndex);
                header.writeInt(offsetInHeaderFrame + 1, offsetInContentFrame);
                contentFrame.writeInt(offsetInContentFrame + 1, entryUsedCountInSlot);
                return false;
            }

            int newFrameIndex = header.getInt(offsetInHeaderFrame);
            int newTupleIndex = header.getInt(offsetInHeaderFrame + 1);

            // Migrates the existing entries (from 2nd to the last).
            for (int i = 1; i < entryUsedCountInSlot; i++) {
                int startOffsetInContentFrame = offsetInContentFrame + 2 + i * 2;
                int startFrameIndex = frameIndex;
                while (startOffsetInContentFrame >= frameCapacity) {
                    ++startFrameIndex;
                    startOffsetInContentFrame -= frameCapacity;
                }
                contentFrame = contents.get(startFrameIndex);
                fIndex = contentFrame.getInt(startOffsetInContentFrame);
                tIndex = contentFrame.getInt(startOffsetInContentFrame + 1);
                tempTuplePointer.reset(fIndex, tIndex);
                if (!insertNonFirstTuple(header, offsetInHeaderFrame, newFrameIndex, newTupleIndex, tempTuplePointer)) {
                    return false;
                }
            }
            // Now, inserts the new entry that caused an overflow to the old bucket.
            if (!insertNonFirstTuple(header, offsetInHeaderFrame, newFrameIndex, newTupleIndex, pointer)) {
                return false;
            }
            increaseWastedSpaceCount(capacity);
        }
        return true;
    }

    protected int getHeaderFrameIndex(int entry) {
        int frameIndex = entry * 2 / frameCapacity;
        return frameIndex;
    }

    protected int getHeaderFrameOffset(int entry) {
        int offset = entry * 2 % frameCapacity;
        return offset;
    }

    public static int getUnitSize() {
        return INT_SIZE;
    }

    public static int getNumberOfEntryInSlot() {
        return INIT_ENTRY_SIZE;
    }

    public static int getExpectedByteSizePerHashValue() {
        // first constant 2: capacity, # of used count
        // second constant 2: tuple pointer (frameIndex, offset)
        return getUnitSize() * (2 + getNumberOfEntryInSlot() * 2);
    }

    /**
     * Calculates the expected hash table size based on a scenario: there are no duplicated entries so that
     * each entry is assigned to all possible slots.
     *
     * @param tableSize
     *            : the cardinality of the hash table - number of slots
     * @param frameSize
     *            : the frame size
     * @return
     *         expected the byte size of the hash table
     */
    public static long getExpectedTableFrameCount(long tableSize, int frameSize) {
        long numberOfHeaderFrame = (long) (Math.ceil((double) tableSize * 2 * getUnitSize() / (double) frameSize));
        long numberOfContentFrame = (long) (Math
                .ceil(((double) getNumberOfEntryInSlot() * 2 * getUnitSize() * tableSize) / (double) frameSize));
        return numberOfHeaderFrame + numberOfContentFrame;
    }

    public static long getExpectedTableByteSize(long tableSize, int frameSize) {
        return getExpectedTableFrameCount(tableSize, frameSize) * frameSize;
    }

    /**
     * Calculates the frame count increment/decrement for a new table size with the original size.
     *
     * @param origTableSize
     *            : the original table cardinality
     * @param delta
     *            : a delta (a positive value means that the cardinality of the table will be increased.)
     *            a negative value means that the cardinality of the table will be decreased.
     * @return the frame count increment/decrement: a positive number means that the table size will be increased.
     *         a negative number means that the table size will be decreased.
     */
    public static long calculateFrameCountDeltaForTableSizeChange(long origTableSize, long delta, int frameSize) {
        long originalFrameCount = getExpectedTableFrameCount(origTableSize, frameSize);
        long newFrameCount = getExpectedTableFrameCount(origTableSize + delta, frameSize);
        return newFrameCount - originalFrameCount;
    }

    public static long calculateByteSizeDeltaForTableSizeChange(long origTableSize, long delta, int frameSize) {
        return calculateFrameCountDeltaForTableSizeChange(origTableSize, delta, frameSize) * frameSize;
    }

    @Override
    public boolean isGarbageCollectionNeeded() {
        // This class doesn't support the garbage collection.
        return false;
    }

    @Override
    public int collectGarbage(ITuplePointerAccessor bufferAccessor, ITuplePartitionComputer tpc)
            throws HyracksDataException {
        // This class doesn't support the garbage collection.
        return -1;
    }

    static class IntSerDeBuffer {

        ByteBuffer byteBuffer;
        byte[] bytes;

        public IntSerDeBuffer(ByteBuffer byteBuffer) {
            this.byteBuffer = byteBuffer;
            this.bytes = byteBuffer.array();
            resetFrame();
        }

        public int getInt(int pos) {
            int offset = pos * 4;
            return ((bytes[offset] & 0xff) << 24) + ((bytes[offset + 1] & 0xff) << 16)
                    + ((bytes[offset + 2] & 0xff) << 8) + (bytes[offset + 3] & 0xff);
        }

        public void writeInt(int pos, int value) {
            int offset = pos * 4;
            bytes[offset++] = (byte) (value >> 24);
            bytes[offset++] = (byte) (value >> 16);
            bytes[offset++] = (byte) (value >> 8);
            bytes[offset] = (byte) (value);
        }

        public void writeInvalidVal(int intPos, int intRange) {
            int offset = intPos * 4;
            Arrays.fill(bytes, offset, offset + INT_SIZE * intRange, INVALID_BYTE_VALUE);
        }

        public int capacity() {
            return bytes.length / 4;
        }

        public int getByteCapacity() {
            return bytes.length;
        }

        public ByteBuffer getByteBuffer() {
            return byteBuffer;
        }

        public void resetFrame() {
            Arrays.fill(bytes, INVALID_BYTE_VALUE);
        }

    }

    @Override
    public String printInfo() {
        return null;
    }

    @Override
    public int getTableSize() {
        return tableSize;
    }
}
