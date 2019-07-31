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
import java.util.Map;
import java.util.TreeMap;

import org.apache.hyracks.api.context.IHyracksFrameMgrContext;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionComputer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.std.buffermanager.ISimpleFrameBufferManager;
import org.apache.hyracks.dataflow.std.buffermanager.ITuplePointerAccessor;

/**
 * This is an extension of SimpleSerializableHashTable class.
 * A buffer manager needs to be assigned to allocate/release frames for this table so that
 * the maximum memory usage can be bounded under the certain limit.
 */
public class SerializableHashTable extends SimpleSerializableHashTable {

    protected double garbageCollectionThreshold;
    protected int wastedIntSpaceCount = 0;
    protected ISimpleFrameBufferManager bufferManager;

    public SerializableHashTable(int tableSize, final IHyracksFrameMgrContext ctx,
            ISimpleFrameBufferManager bufferManager) throws HyracksDataException {
        this(tableSize, ctx, bufferManager, 0.1);
    }

    public SerializableHashTable(int tableSize, final IHyracksFrameMgrContext ctx,
            ISimpleFrameBufferManager bufferManager, double garbageCollectionThreshold) throws HyracksDataException {
        super(tableSize, ctx, false);
        this.bufferManager = bufferManager;
        if (tableSize > 0) {
            ByteBuffer newFrame = getFrame(frameSize);
            if (newFrame == null) {
                throw new HyracksDataException("Can't allocate a frame for Hash Table. Please allocate more budget.");
            }
            IntSerDeBuffer frame = new IntSerDeBuffer(newFrame);
            frameCapacity = frame.capacity();
            contents.add(frame);
            currentOffsetInEachFrameList.add(0);
        }
        this.garbageCollectionThreshold = garbageCollectionThreshold;
    }

    @Override
    ByteBuffer getFrame(int size) throws HyracksDataException {
        ByteBuffer newFrame = bufferManager.acquireFrame(size);
        if (newFrame != null) {
            currentByteSize += size;
        }
        return newFrame;
    }

    @Override
    void increaseWastedSpaceCount(int size) {
        wastedIntSpaceCount += size;
    }

    @Override
    public void reset() {
        super.reset();
        currentByteSize = 0;
    }

    @Override
    public void close() {
        for (int i = 0; i < headers.length; i++) {
            if (headers[i] != null) {
                bufferManager.releaseFrame(headers[i].getByteBuffer());
                headers[i] = null;
            }
        }
        for (int i = 0; i < contents.size(); i++) {
            bufferManager.releaseFrame(contents.get(i).getByteBuffer());
        }
        contents.clear();
        currentOffsetInEachFrameList.clear();
        tupleCount = 0;
        currentByteSize = 0;
        currentLargestFrameNumber = 0;
    }

    @Override
    public boolean isGarbageCollectionNeeded() {
        return wastedIntSpaceCount > frameCapacity * (currentLargestFrameNumber + 1) * garbageCollectionThreshold;
    }

    /**
     * Collects garbages. The steps are as follows.
     * #1. Initialize the Reader and Writer. The starting frame index is set to zero at this moment.
     * #2. Read a content frame. Find and read a slot data. Check the number of used count for the slot.
     * If it's not -1 (meaning that it is being used now), we move it to to the
     * current writing offset of the Writer frame. Update the corresponding h() value pointer for this location
     * in the header frame. We can find the h() value of the slot using a first tuple pointer in the slot.
     * If the number is -1 (meaning that it is migrated to a new place due to an overflow or deleted),
     * just reclaim the space by letting other slot move to this space.
     * #3. Once a Reader reaches the end of a frame, read next frame by frame. This applies to the Writer, too. i.e.
     * If the writing offset pointer reaches at the end of a frame, then writing frame will be set to the next frame.
     * #4. Repeat #1 ~ #3 until all frames are read.
     *
     * @return the number of frames that are reclaimed. The value -1 is returned when no compaction was happened.
     */
    @Override
    public int collectGarbage(ITuplePointerAccessor bufferAccessor, ITuplePartitionComputer tpc)
            throws HyracksDataException {
        // Keeps the garbage collection related variable
        GarbageCollectionInfo gcInfo = new GarbageCollectionInfo();

        int slotCapacity;
        int slotUsedCount;
        int capacityInIntCount;
        int nextSlotIntPosInPageForGC;
        boolean currentPageChanged;
        IntSerDeBuffer currentReadContentFrameForGC;
        IntSerDeBuffer currentWriteContentFrameForGC = contents.get(gcInfo.currentGCWritePageForGC);
        int lastOffsetInLastFrame = currentOffsetInEachFrameList.get(contents.size() - 1);

        // Step #1. Reads a content frame until it reaches the end of content frames.
        while (gcInfo.currentReadPageForGC <= currentLargestFrameNumber) {

            gcInfo.currentReadIntOffsetInPageForGC = 0;
            currentReadContentFrameForGC = contents.get(gcInfo.currentReadPageForGC);

            // Step #2. Advances the reader until it hits the end of the given frame.
            while (gcInfo.currentReadIntOffsetInPageForGC < frameCapacity) {
                nextSlotIntPosInPageForGC =
                        findNextSlotInPage(currentReadContentFrameForGC, gcInfo.currentReadIntOffsetInPageForGC);

                if (nextSlotIntPosInPageForGC == INVALID_VALUE) {
                    // There isn't a valid slot in the page. Exits the loop #2 and reads the next frame.
                    break;
                }

                // Valid slot found. Reads the given slot information.
                slotCapacity = currentReadContentFrameForGC.getInt(nextSlotIntPosInPageForGC);
                slotUsedCount = currentReadContentFrameForGC.getInt(nextSlotIntPosInPageForGC + 1);
                capacityInIntCount = (slotCapacity + 1) * 2;

                // Used count should not be -1 (migrated or deleted).
                if (slotUsedCount != INVALID_VALUE) {
                    // To prepare hash pointer (header -> content) update, read the first tuple pointer in the old slot.
                    tempTuplePointer.reset(currentReadContentFrameForGC.getInt(nextSlotIntPosInPageForGC + 2),
                            currentReadContentFrameForGC.getInt(nextSlotIntPosInPageForGC + 3));

                    // Check whether there is at least some space to put some part of the slot.
                    // If not, advance the write pointer to the next page.
                    if ((gcInfo.currentWriteIntOffsetInPageForGC + 4) > frameCapacity
                            && gcInfo.currentGCWritePageForGC < currentLargestFrameNumber) {
                        // Swipe the region that can't be used.
                        currentWriteContentFrameForGC.writeInvalidVal(gcInfo.currentWriteIntOffsetInPageForGC,
                                frameCapacity - gcInfo.currentWriteIntOffsetInPageForGC);
                        gcInfo.currentGCWritePageForGC++;
                        currentWriteContentFrameForGC = contents.get(gcInfo.currentGCWritePageForGC);
                        gcInfo.currentWriteIntOffsetInPageForGC = 0;
                    }

                    // Migrates this slot to the current offset in Writer's Frame if possible.
                    currentPageChanged =
                            MigrateSlot(gcInfo, bufferAccessor, tpc, capacityInIntCount, nextSlotIntPosInPageForGC);

                    if (currentPageChanged) {
                        currentReadContentFrameForGC = contents.get(gcInfo.currentReadPageForGC);
                        currentWriteContentFrameForGC = contents.get(gcInfo.currentGCWritePageForGC);
                    }
                } else {
                    // A useless slot (either migrated or deleted) is found. Resets the space
                    // so it will be occupied by the next valid slot.
                    currentPageChanged = resetSlotSpace(gcInfo, nextSlotIntPosInPageForGC, capacityInIntCount);

                    if (currentPageChanged) {
                        currentReadContentFrameForGC = contents.get(gcInfo.currentReadPageForGC);
                    }

                }
            }

            // Reached the end of a frame. Advances the Reader.
            if (gcInfo.currentReadPageForGC == currentLargestFrameNumber) {
                break;
            }
            gcInfo.currentReadPageForGC++;
        }

        // More unused frames at the end?
        int extraFrames = 0;
        if (contents.size() > (currentLargestFrameNumber + 1)) {
            extraFrames = contents.size() - (currentLargestFrameNumber + 1);
        }

        // Done reading all frames. So, releases unnecessary frames.
        int numberOfFramesToBeDeallocated = gcInfo.currentReadPageForGC + extraFrames - gcInfo.currentGCWritePageForGC;

        if (numberOfFramesToBeDeallocated >= 1) {
            for (int i = 0; i < numberOfFramesToBeDeallocated; i++) {
                currentByteSize -= contents.get(gcInfo.currentGCWritePageForGC + 1).getByteCapacity();
                bufferManager.releaseFrame(contents.get(gcInfo.currentGCWritePageForGC + 1).getByteBuffer());
                contents.remove(gcInfo.currentGCWritePageForGC + 1);
                currentOffsetInEachFrameList.remove(gcInfo.currentGCWritePageForGC + 1);
            }
        } else {
            // For this case, we check whether the last offset is changed.
            // If not, we didn't get any space from the operation.
            int afterLastOffsetInLastFrame = currentOffsetInEachFrameList.get(gcInfo.currentGCWritePageForGC);
            if (lastOffsetInLastFrame == afterLastOffsetInLastFrame) {
                numberOfFramesToBeDeallocated = -1;
            }
        }

        // Resets the current offset in the last frame so that the future insertions will work without an issue.
        currentLargestFrameNumber = gcInfo.currentGCWritePageForGC;
        currentOffsetInEachFrameList.set(gcInfo.currentGCWritePageForGC, gcInfo.currentWriteIntOffsetInPageForGC);

        wastedIntSpaceCount = 0;
        tempTuplePointer.reset(INVALID_VALUE, INVALID_VALUE);

        return numberOfFramesToBeDeallocated;
    }

    /**
     * Migrates the current slot to the designated place and reset the current space using INVALID_VALUE.
     *
     * @return true if the current page has been changed. false if not.
     */
    private boolean MigrateSlot(GarbageCollectionInfo gcInfo, ITuplePointerAccessor bufferAccessor,
            ITuplePartitionComputer tpc, int capacityInIntCount, int nextSlotIntPosInPageForGC)
            throws HyracksDataException {
        boolean currentPageChanged = false;
        // If the reader and writer indicate the same slot location, a move is not required.
        if (gcInfo.isReaderWriterAtTheSamePos()) {
            int intToRead = capacityInIntCount;
            int intReadAtThisTime;
            gcInfo.currentReadIntOffsetInPageForGC = nextSlotIntPosInPageForGC;
            while (intToRead > 0) {
                intReadAtThisTime = Math.min(intToRead, frameCapacity - gcInfo.currentReadIntOffsetInPageForGC);
                gcInfo.currentReadIntOffsetInPageForGC += intReadAtThisTime;
                if (gcInfo.currentReadIntOffsetInPageForGC >= frameCapacity
                        && gcInfo.currentReadPageForGC < currentLargestFrameNumber) {
                    gcInfo.currentReadPageForGC++;
                    gcInfo.currentReadIntOffsetInPageForGC = 0;
                    currentPageChanged = true;
                }
                intToRead -= intReadAtThisTime;
            }

            gcInfo.currentGCWritePageForGC = gcInfo.currentReadPageForGC;
            gcInfo.currentWriteIntOffsetInPageForGC = gcInfo.currentReadIntOffsetInPageForGC;

            return currentPageChanged;
        }

        // The reader is ahead of the writer. We can migrate the given slot towards to the beginning of
        // the content frame(s).
        int tempWriteIntPosInPage = gcInfo.currentWriteIntOffsetInPageForGC;
        int tempReadIntPosInPage = nextSlotIntPosInPageForGC;
        int chunksToMove = capacityInIntCount;
        int chunksToMoveAtThisTime;

        // To keep the original writing page that is going to be used for updating the header to content frame,
        // we declare a local variable.
        int tempWritePage = gcInfo.currentGCWritePageForGC;

        // Keeps the maximum INT chunks that writer/reader can write in the current page.
        int oneTimeIntCapacityForWriter;
        int oneTimeIntCapacityForReader;

        IntSerDeBuffer currentReadContentFrameForGC = contents.get(gcInfo.currentReadPageForGC);
        IntSerDeBuffer currentWriteContentFrameForGC = contents.get(gcInfo.currentGCWritePageForGC);

        // Moves the slot.
        while (chunksToMove > 0) {
            oneTimeIntCapacityForWriter = Math.min(chunksToMove, frameCapacity - tempWriteIntPosInPage);
            oneTimeIntCapacityForReader = Math.min(chunksToMove, frameCapacity - tempReadIntPosInPage);

            // Since the location of Reader and Writer are different, we can only move a minimum chunk
            // before the current page of either Reader or Writer changes.
            chunksToMoveAtThisTime = Math.min(oneTimeIntCapacityForWriter, oneTimeIntCapacityForReader);

            // Moves a part of the slot from the Reader to Writer
            System.arraycopy(currentReadContentFrameForGC.bytes, tempReadIntPosInPage * INT_SIZE,
                    currentWriteContentFrameForGC.bytes, tempWriteIntPosInPage * INT_SIZE,
                    chunksToMoveAtThisTime * INT_SIZE);

            // Clears that part in the Reader
            for (int i = 0; i < chunksToMoveAtThisTime; i++) {
                // Do not blindly put -1 since there might be overlapping between writer and reader.
                if ((gcInfo.currentReadPageForGC != tempWritePage)
                        || (tempReadIntPosInPage + i >= tempWriteIntPosInPage + chunksToMoveAtThisTime)) {
                    currentReadContentFrameForGC.writeInvalidVal(tempReadIntPosInPage + i, chunksToMoveAtThisTime - i);
                    break;
                }
            }

            // Advances the pointer
            tempWriteIntPosInPage += chunksToMoveAtThisTime;
            tempReadIntPosInPage += chunksToMoveAtThisTime;

            // Once the writer pointer hits the end of the page, we move to the next content page.
            if (tempWriteIntPosInPage >= frameCapacity && tempWritePage < currentLargestFrameNumber) {
                tempWritePage++;
                currentPageChanged = true;
                currentWriteContentFrameForGC = contents.get(tempWritePage);
                tempWriteIntPosInPage = 0;
            }

            // Once the reader pointer hits the end of the page, we move to the next content page.
            if (tempReadIntPosInPage >= frameCapacity && gcInfo.currentReadPageForGC < currentLargestFrameNumber) {
                gcInfo.currentReadPageForGC++;
                currentPageChanged = true;
                currentReadContentFrameForGC = contents.get(gcInfo.currentReadPageForGC);
                tempReadIntPosInPage = 0;
            }

            chunksToMove -= chunksToMoveAtThisTime;
        }

        updateHeaderToContentPointerInHeaderFrame(bufferAccessor, tpc, tempTuplePointer, gcInfo.currentGCWritePageForGC,
                gcInfo.currentWriteIntOffsetInPageForGC);

        gcInfo.currentGCWritePageForGC = tempWritePage;
        gcInfo.currentWriteIntOffsetInPageForGC = tempWriteIntPosInPage;
        gcInfo.currentReadIntOffsetInPageForGC = tempReadIntPosInPage;

        return currentPageChanged;
    }

    /**
     * Completely removes the slot in the given content frame(s) and resets the space.
     * For this method, we assume that this slot is not moved to somewhere else.
     *
     * @return true if the current page has been changed. false if not.
     */
    private boolean resetSlotSpace(GarbageCollectionInfo gcInfo, int slotIntPos, int capacityInIntCount) {
        boolean currentPageChanged = false;
        int tempReadIntPosInPage = slotIntPos;
        int chunksToDelete = capacityInIntCount;
        int chunksToDeleteAtThisTime;
        IntSerDeBuffer currentReadContentFrameForGC = contents.get(gcInfo.currentReadPageForGC);

        while (chunksToDelete > 0) {
            chunksToDeleteAtThisTime = Math.min(chunksToDelete, frameCapacity - tempReadIntPosInPage);

            // Clears that part in the Reader
            currentReadContentFrameForGC.writeInvalidVal(tempReadIntPosInPage, chunksToDeleteAtThisTime);

            // Advances the pointer
            tempReadIntPosInPage += chunksToDeleteAtThisTime;

            // Once the reader pointer hits the end of the page, we move to the next content page.
            if (tempReadIntPosInPage >= frameCapacity && gcInfo.currentReadPageForGC < currentLargestFrameNumber) {
                gcInfo.currentReadPageForGC++;
                currentPageChanged = true;
                currentReadContentFrameForGC = contents.get(gcInfo.currentReadPageForGC);
                tempReadIntPosInPage = 0;
            }

            chunksToDelete -= chunksToDeleteAtThisTime;
        }

        gcInfo.currentReadIntOffsetInPageForGC = tempReadIntPosInPage;

        return currentPageChanged;
    }

    /**
     * Updates the given Header to Content Frame Pointer after calculating the corresponding hash value from the
     * given tuple pointer.
     */
    private void updateHeaderToContentPointerInHeaderFrame(ITuplePointerAccessor bufferAccessor,
            ITuplePartitionComputer tpc, TuplePointer hashedTuple, int newContentFrame, int newOffsetInContentFrame)
            throws HyracksDataException {
        // Finds the original hash value. We assume that bufferAccessor and tpc is already assigned.
        bufferAccessor.reset(hashedTuple);
        int entry = tpc.partition(bufferAccessor, hashedTuple.getTupleIndex(), tableSize);

        // Finds the location of the hash value in the header frame arrays.
        int headerFrameIndex = getHeaderFrameIndex(entry);
        int offsetInHeaderFrame = getHeaderFrameOffset(entry);
        IntSerDeBuffer headerFrame = headers[headerFrameIndex];

        // Updates the hash value.
        headerFrame.writeInt(offsetInHeaderFrame, newContentFrame);
        headerFrame.writeInt(offsetInHeaderFrame + 1, newOffsetInContentFrame);
    }

    /**
     * Tries to find the next valid slot position in the given content frame from the current position.
     */
    private int findNextSlotInPage(IntSerDeBuffer frame, int readIntPosAtPage) {
        // Sanity check
        if (readIntPosAtPage >= frameCapacity) {
            return INVALID_VALUE;
        }
        int intOffset = readIntPosAtPage;
        while (frame.getInt(intOffset) == INVALID_VALUE) {
            intOffset++;
            if (intOffset >= frameCapacity) {
                // Couldn't find the next slot in the given page.
                return INVALID_VALUE;
            }
        }
        return intOffset;
    }

    /**
     * Keeps the garbage collection related variables
     */
    private static class GarbageCollectionInfo {
        int currentReadPageForGC;
        int currentReadIntOffsetInPageForGC;
        int currentGCWritePageForGC;
        int currentWriteIntOffsetInPageForGC;

        public GarbageCollectionInfo() {
            currentReadPageForGC = 0;
            currentReadIntOffsetInPageForGC = 0;
            currentGCWritePageForGC = 0;
            currentWriteIntOffsetInPageForGC = 0;
        }

        /**
         * Checks whether the writing position and the reading position are the same.
         */
        public boolean isReaderWriterAtTheSamePos() {
            return currentReadPageForGC == currentGCWritePageForGC
                    && currentReadIntOffsetInPageForGC == currentWriteIntOffsetInPageForGC;
        }
    }

    /**
     * Returns the current status of this table: the number of slots, frames, space utilization, etc.
     */
    @Override
    public String printInfo() {
        SlotInfoPair<Integer, Integer> slotInfo = new SlotInfoPair<>(0, 0);

        int nFrames = contents.size();
        int hFrames = 0;
        // Histogram Information - counts the number of used count per slot used count (e.g., 10,2 means that
        // there are 10 hash slots that only has two hash entries in it.)
        Map<Integer, Integer> headerSlotUsedCountMap = new TreeMap<>();

        // Histogram Information - counts the number of capacity count per slot count (10,3 means that
        // there are 10 hash slots whose capacity is 3.)
        Map<Integer, Integer> headerSlotCapaCountMap = new TreeMap<>();

        int headerSlotUsedCount = 0;
        int headerSlotTotalCount;
        double headerSlotUsedRatio = 0.0;
        IntSerDeBuffer header;
        int tupleUsedCount;
        int tupleUsedCountFromMap;
        int capacity;
        int capacityFromMap;
        for (int i = 0; i < headers.length; i++) {
            if (headers[i] != null) {
                header = headers[i];
                for (int j = 0; j < frameCapacity; j = j + 2) {
                    if (header.getInt(j) >= 0) {
                        headerSlotUsedCount++;
                        getSlotInfo(header.getInt(j), header.getInt(j + 1), slotInfo);
                        capacity = slotInfo.first;
                        tupleUsedCount = slotInfo.second;
                        // UsedCount increase
                        if (headerSlotUsedCountMap.containsKey(tupleUsedCount)) {
                            tupleUsedCountFromMap = headerSlotUsedCountMap.get(tupleUsedCount);
                            headerSlotUsedCountMap.put(tupleUsedCount, tupleUsedCountFromMap + 1);
                        } else {
                            headerSlotUsedCountMap.put(tupleUsedCount, 1);
                        }
                        // Capacity increase
                        if (headerSlotCapaCountMap.containsKey(capacity)) {
                            capacityFromMap = headerSlotCapaCountMap.get(capacity);
                            headerSlotCapaCountMap.put(capacity, capacityFromMap + 1);
                        } else {
                            headerSlotCapaCountMap.put(capacity, 1);
                        }
                        headerSlotUsedCount++;
                    }
                }
                hFrames++;
            }
        }
        headerSlotTotalCount = hFrames * frameCapacity / 2;
        if (headerSlotTotalCount > 0) {
            headerSlotUsedRatio = (double) headerSlotUsedCount / (double) headerSlotTotalCount;
        }
        int total = hFrames + nFrames;
        StringBuilder buf = new StringBuilder();
        buf.append("\n>>> " + this + " " + Thread.currentThread().getId() + "::printInfo()" + "\n");
        buf.append("(A) hash table cardinality (# of slot):\t" + tableSize + "\tExpected Table Size(MB):\t"
                + ((double) getExpectedTableByteSize(tableSize, frameCapacity * 4) / 1048576) + "\twasted size(MB):\t"
                + ((double) wastedIntSpaceCount * 4 / 1048576) + "\n");
        buf.append("(B) # of header frames:\t" + hFrames + "\tsize(MB)\t"
                + ((double) hFrames * frameCapacity * 4 / 1048576) + "\tratio (B/D)\t" + ((double) hFrames / total)
                + "\n");
        buf.append("(C) # of content frames:\t" + nFrames + "\tsize(MB)\t"
                + ((double) nFrames * frameCapacity * 4 / 1048576) + "\tratio (C/D)\t" + ((double) nFrames / total)
                + "\n");
        buf.append("(D) # of total frames:\t" + total + "\tsize(MB)\t" + ((double) total * frameCapacity * 4 / 1048576)
                + "\n");
        buf.append("(E) # of used header entries:\t" + headerSlotUsedCount + "\n");
        buf.append("(F) # of all possible header entries:\t" + headerSlotTotalCount + "\n");
        buf.append("(G) header entries used ratio (E/F):\t" + headerSlotUsedRatio + "\n");
        buf.append("(H) used count histogram (used count, its frequency):" + "\n");
        int totalContentUsedCount = 0;
        for (Map.Entry<Integer, Integer> entry : headerSlotUsedCountMap.entrySet()) {
            buf.append(entry.getKey() + "\t" + entry.getValue() + "\n");
            totalContentUsedCount += (entry.getKey() * entry.getValue());
        }
        buf.append("(H-1) total used count in content frames:\t" + totalContentUsedCount + "\n");

        int totalContentCapaCount = 0;
        buf.append("(I) capacity count histogram (capacity, its frequency):" + "\n");
        for (Map.Entry<Integer, Integer> entry : headerSlotCapaCountMap.entrySet()) {
            buf.append(entry.getKey() + "\t" + entry.getValue() + "\n");
            totalContentCapaCount += (entry.getKey() * entry.getValue());
        }
        buf.append("(I-1) total capacity in content frames:\t" + totalContentCapaCount + "\n");
        buf.append("(J) ratio of used count in content frames (H-1 / I-1):\t"
                + ((double) totalContentUsedCount / totalContentCapaCount) + "\n");
        return buf.toString();
    }

    /**
     * Returns the capacity and the usedCount for the given slot in this table.
     */
    public void getSlotInfo(int contentFrameIndex, int contentOffsetIndex, SlotInfoPair<Integer, Integer> slotInfo) {
        IntSerDeBuffer frame = contents.get(contentFrameIndex);
        int entryCapacity = frame.getInt(contentOffsetIndex);
        int entryUsedItems = frame.getInt(contentOffsetIndex + 1);
        slotInfo.reset(entryCapacity, entryUsedItems);
    }

    private static class SlotInfoPair<T1, T2> {

        private T1 first;
        private T2 second;

        public SlotInfoPair(T1 first, T2 second) {
            this.first = first;
            this.second = second;
        }

        public void reset(T1 first, T2 second) {
            this.first = first;
            this.second = second;
        }
    }
}
