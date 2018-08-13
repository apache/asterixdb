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
package org.apache.hyracks.data.std.util;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunction;
import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * The most simplest implementation of a static hash-set you could imagine.
 * Intended to work with binary data and be able to map arbitrary key types to
 * arbitrary value types, given that they have implementations of
 * IBinaryHashFunction and IBinaryComparator.
 * Each key in the hash table: the offset (2 byte), length of an entry (2 byte).
 * The real key value is not stored in the set since it can be found using the reference array.
 * Additionally, it has the count (1 byte) in a byte array.
 * Hash value: based on an entry value, it will be calculated.
 * This class is NOT thread safe. - For single thread access only
 * Limitation - a frame size can't be greater than 64K because we use 2 bytes to store the offset.
 * Can't have more than 64K frames.
 */
public class BinaryHashSet {
    // Special value to indicate an empty "bucket" in the header array.
    static final int NULL_PTR = -1;
    private static final int PTR_SIZE = 4; // 2 byte - frameIdx, 2 byte - frameOffset
    static final int SLOT_SIZE = 2;

    // This hash-set also stores the count of the real key.
    // It's not part of the key and can be used to indicate whether this key exists in a different array or not.
    static final int COUNT_SIZE = 1; // max value: Byte.MAX_VALUE (2^7 - 1)
    private static final int ENTRY_HEADER_SIZE = 2 * SLOT_SIZE + PTR_SIZE + COUNT_SIZE;
    // We are using 2 byte. Therefore, the limit is 64K.
    private static final int NO_OF_FRAME_LIMIT = 65535;
    private static final int ONE_FRAME_SIZE_LIMIT = 65535;
    private final IBinaryHashFunction hashFunc;
    private final IBinaryComparator cmp;

    private final int[] listHeads;
    private final int frameSize;
    private final List<ByteBuffer> frames = new ArrayList<>();
    private int currFrameIndex;
    private int nextOff;
    private int size;

    // Byte array that holds the real data for this hashset
    private byte[] refArray;

    // Initialize a hash-set. It will contain one frame by default.
    public BinaryHashSet(int tableSize, int frameSize, IBinaryHashFunction hashFunc, IBinaryComparator cmp,
            byte[] refArray) {
        listHeads = new int[tableSize];
        if (frameSize > ONE_FRAME_SIZE_LIMIT) {
            throw new IllegalStateException(
                    "A frame size can't be greater than " + ONE_FRAME_SIZE_LIMIT + ". Can't continue.");
        }
        this.frameSize = frameSize;
        this.hashFunc = hashFunc;
        this.cmp = cmp;
        frames.add(ByteBuffer.allocate(frameSize));
        clear();
        this.refArray = refArray;
    }

    /**
     * Set the byte array that the keys in this hash-set refer to.
     *
     * @param refArray
     */
    public void setRefArray(byte[] refArray) {
        this.refArray = refArray;
    }

    /**
     * Inserts a key (off, len) into the hash set.
     * The count of the key will not be changed.
     *
     * @param key
     * @return the current count of the key: when a given key is inserted or that key is already there.
     *         0: when an insertion succeeds.
     * @throws HyracksDataException
     */
    public int put(BinaryEntry key) throws HyracksDataException {
        return putFindInternal(key, true, null, false);
    }

    /**
     * Find whether the given key from an array exists in the hash set.
     * If the key exists, then the count will be increased by 1.
     *
     * @param key
     * @param keyArray
     * @param increaseFoundCount
     * @return the current count of the key: when a given key exists.
     *         -1: when the given key doesn't exist.
     * @throws HyracksDataException
     */
    public int find(BinaryEntry key, byte[] keyArray, boolean increaseFoundCount) throws HyracksDataException {
        return putFindInternal(key, false, keyArray, increaseFoundCount);
    }

    // Put an entry or find an entry
    private int putFindInternal(BinaryEntry key, boolean isInsert, byte[] keyArray, boolean increaseFoundCount)
            throws HyracksDataException {
        int bucket;
        bucket = isInsert ? Math.abs(hashFunc.hash(this.refArray, key.getOffset(), key.getLength()) % listHeads.length)
                : Math.abs(hashFunc.hash(keyArray, key.getOffset(), key.getLength()) % listHeads.length);

        int headPtr = listHeads[bucket];
        if (headPtr == NULL_PTR) {
            // Key definitely doesn't exist yet.
            if (isInsert) {
                // Key is being inserted.
                listHeads[bucket] = appendEntry(key);
                return 0;
            } else {
                // find case - the bucket is empty: return false since there is no element in the hash-set
                return -1;
            }

        }
        // if headPtr is not null,
        // follow the chain in the bucket until we found an entry matching the given key.
        int frameNum;
        int frameOff;
        int entryKeyOff;
        int entryKeyLen;
        int entryCount;
        ByteBuffer frame;
        do {
            // Get frame num and frame offset from the ptr
            frameNum = getFrameIndex(headPtr);
            frameOff = getFrameOffset(headPtr);
            frame = frames.get(frameNum);

            // Get entry offset
            entryKeyOff = (int) frame.getChar(frameOff);
            entryKeyLen = (int) frame.getChar(frameOff + SLOT_SIZE);

            // Check the key length. If they don't match, we don't even need to compare two entries.
            if (entryKeyLen == key.getLength()) {
                if (isInsert) {
                    if (cmp.compare(this.refArray, entryKeyOff, entryKeyLen, this.refArray, key.getOffset(),
                            key.getLength()) == 0) {
                        // put - Key found, return true since we return true when the key is already in the hash-map.
                        entryCount = (int) frame.get(frameOff + 2 * SLOT_SIZE);
                        return entryCount;
                    }
                } else if (cmp.compare(this.refArray, entryKeyOff, entryKeyLen, keyArray, key.getOffset(),
                        key.getLength()) == 0) {
                    // Find case - the key is found, increase the count when increaseCount is set to true.
                    // Return the count. The maximum count is Byte.MAX_VALUE.
                    entryCount = (int) frame.get(frameOff + 2 * SLOT_SIZE);
                    if (increaseFoundCount && entryCount < Byte.MAX_VALUE) {
                        entryCount++;
                    }
                    frame.put(frameOff + 2 * SLOT_SIZE, (byte) entryCount);
                    return entryCount;
                }
            }
            // Get next key position
            headPtr = frame.getInt(frameOff + 2 * SLOT_SIZE + COUNT_SIZE);
        } while (headPtr != NULL_PTR);

        // We've followed the chain to its end, and didn't find the key.
        if (isInsert) {
            // Append the new entry, and set a pointer to it in the last entry we've checked.
            // put case - success
            int newPtr = appendEntry(key);
            frame.putInt(frameOff + 2 * SLOT_SIZE + COUNT_SIZE, newPtr);
            return 0;
        } else {
            // find case - fail
            return -1;
        }
    }

    public int appendEntry(BinaryEntry key) {
        ByteBuffer frame = frames.get(currFrameIndex);
        int requiredSpace = ENTRY_HEADER_SIZE;
        if (nextOff + requiredSpace >= frameSize) {
            // Entry doesn't fit on the current frame, allocate a new one.
            if (requiredSpace > frameSize) {
                throw new IllegalStateException(
                        "A hash key is greater than the framesize: " + frameSize + ". Can't continue.");
            } else if (frames.size() > NO_OF_FRAME_LIMIT) {
                throw new IllegalStateException(
                        "There can't be more than " + NO_OF_FRAME_LIMIT + "frames. Can't continue.");
            }
            frames.add(ByteBuffer.allocate(frameSize));
            currFrameIndex++;
            nextOff = 0;
            frame = frames.get(currFrameIndex);
        }
        writeEntryHeader(frame, nextOff, key.getOffset(), key.getLength(), 0, NULL_PTR);
        int entryPtr = getEntryPtr(currFrameIndex, nextOff);
        nextOff += requiredSpace;
        size++;
        return entryPtr;
    }

    private void writeEntryHeader(ByteBuffer frame, int targetOff, int keyOff, int keyLen, int keyCount, int ptr) {
        // [2 byte key offset] [2 byte key length] [1 byte key count] [2 byte the frame num] [2 byte the frame offset]
        frame.putChar(targetOff, (char) keyOff);
        frame.putChar(targetOff + SLOT_SIZE, (char) keyLen);
        frame.put(targetOff + 2 * SLOT_SIZE, (byte) keyCount);
        frame.putInt(targetOff + 2 * SLOT_SIZE + COUNT_SIZE, ptr);
    }

    private int getEntryPtr(int frameIndex, int frameOff) {
        return (frameIndex << 16) + frameOff;
    }

    private int getFrameIndex(int ptr) {
        return ptr >> 16;
    }

    private int getFrameOffset(int ptr) {
        return ptr & 0xffff;
    }

    public int size() {
        return size;
    }

    public boolean isEmpty() {
        return size > 0;
    }

    public void clear() {
        // Initialize all entries to point to nothing.
        Arrays.fill(listHeads, NULL_PTR);
        currFrameIndex = 0;
        nextOff = 0;
        size = 0;
        this.refArray = null;
    }

    /**
     * Iterates all key entries and resets the foundCount of each key to zero.
     */
    public void clearFoundCount() {
        ByteBuffer frame;
        int frameNum;
        int frameOff;
        int headPtr;
        final int resetCount = 0;

        for (int currentListHeadIndex = 0; currentListHeadIndex < listHeads.length; currentListHeadIndex++) {
            // Position to first non-null list-head pointer.
            if (listHeads[currentListHeadIndex] == NULL_PTR) {
                continue;
            }
            headPtr = listHeads[currentListHeadIndex];
            do {
                // Get frame num and frame offset from the ptr
                frameNum = getFrameIndex(headPtr);
                frameOff = getFrameOffset(headPtr);
                frame = frames.get(frameNum);

                // Set the count as zero
                frame.put(frameOff + 2 * SLOT_SIZE, (byte) resetCount);

                // Get next key position
                headPtr = frame.getInt(frameOff + 2 * SLOT_SIZE + COUNT_SIZE);
            } while (headPtr != NULL_PTR);
        }
    }

}
