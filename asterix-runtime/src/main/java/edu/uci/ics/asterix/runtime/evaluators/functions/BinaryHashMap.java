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
package edu.uci.ics.asterix.runtime.evaluators.functions;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunction;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

/**
 * The most simple implementation of a static hashtable you could imagine.
 * Intended to work with binary data and be able to map arbitrary key types to
 * arbitrary value types, given that they have implementations of
 * IBinaryHashFunction and IBinaryComparator.
 * Uses 2 bytes each to indicate the length of the key and the value.
 * Uses 8 byte pointers for the linked list (4 bytes frame index, 4 bytes frame offset).
 * This class is NOT thread safe.
 */
public class BinaryHashMap {
    // Special value to indicate an empty "bucket" in the header array.
    private static final long NULL_PTR = -1;
    private static final int PTR_SIZE = 8;
    private static final int SLOT_SIZE = 2;
    private static final int ENTRY_HEADER_SIZE = PTR_SIZE + 2 * SLOT_SIZE;
    private final IBinaryHashFunction putHashFunc;
    private final IBinaryHashFunction getHashFunc;
    private final IBinaryComparator cmp;
    private final BinaryEntry returnValue = new BinaryEntry();

    private final long[] listHeads;
    private final int frameSize;
    private final List<ByteBuffer> frames = new ArrayList<ByteBuffer>();
    private int currFrameIndex;
    private int nextOff;
    private int size;

    // Can be used for key or value.
    public static class BinaryEntry {
        public byte[] buf;
        public int off;
        public int len;

        public void set(byte[] buf, int off, int len) {
            this.buf = buf;
            this.off = off;
            this.len = len;
        }

        // Inefficient. Just for debugging.
        @SuppressWarnings("rawtypes")
        public String print(ISerializerDeserializer serde) throws HyracksDataException {
            ByteArrayInputStream inStream = new ByteArrayInputStream(buf, off, len);
            DataInput dataIn = new DataInputStream(inStream);
            return serde.deserialize(dataIn).toString();
        }
    }

    public BinaryHashMap(int tableSize, int frameSize, IBinaryHashFunction putHashFunc,
            IBinaryHashFunction getHashFunc, IBinaryComparator cmp) {
        listHeads = new long[tableSize];
        this.frameSize = frameSize;
        this.putHashFunc = putHashFunc;
        this.getHashFunc = getHashFunc;
        this.cmp = cmp;
        frames.add(ByteBuffer.allocate(frameSize));
        clear();
    }

    /**
     * Inserts key, value into the hash map. If key already exists, returns
     * existing entry. Otherwise, returns null.
     * 
     * @param key
     * @param value
     * @return
     */
    public BinaryEntry put(BinaryEntry key, BinaryEntry value) {
        return getPutInternal(key, value, true);
    }

    /**
     * Retrieves value for given key. Returns null if key doesn't exist.
     * 
     * @param key
     * @param value
     * @return
     */
    public BinaryEntry get(BinaryEntry key) {
        return getPutInternal(key, null, false);
    }

    private BinaryEntry getPutInternal(BinaryEntry key, BinaryEntry value, boolean put) {
        int bucket;
        if (put) {
            bucket = Math.abs(putHashFunc.hash(key.buf, key.off, key.len) % listHeads.length);
        } else {
            bucket = Math.abs(getHashFunc.hash(key.buf, key.off, key.len) % listHeads.length);
        }
        long headPtr = listHeads[bucket];
        if (headPtr == NULL_PTR) {
            // Key definitely doesn't exist yet.
            if (put) {
                listHeads[bucket] = appendEntry(key, value);
            }
            return null;
        }
        // Follow the chain until we found an entry matching the given key.
        int frameOff;
        ByteBuffer frame;
        do {
            int frameIndex = getFrameIndex(headPtr);
            frameOff = getFrameOffset(headPtr);
            frame = frames.get(frameIndex);
            int entryKeyOff = frameOff + ENTRY_HEADER_SIZE;
            int entryKeyLen = frame.getShort(frameOff);
            if (cmp.compare(frame.array(), entryKeyOff, entryKeyLen, key.buf, key.off, key.len) == 0) {
                // Key found, set values and return.
                int entryValOff = frameOff + ENTRY_HEADER_SIZE + entryKeyLen;
                int entryValLen = frame.getShort(frameOff + SLOT_SIZE);
                returnValue.set(frame.array(), entryValOff, entryValLen);
                return returnValue;
            }
            headPtr = frame.getLong(frameOff + 2 * SLOT_SIZE);
        } while (headPtr != NULL_PTR);
        // We've followed the chain to its end, and didn't find the key.
        if (put) {
            // Append the new entry, and set a pointer to it in the last entry we've checked.
            long newPtr = appendEntry(key, value);
            frame.putLong(frameOff + 2 * SLOT_SIZE, newPtr);
        }
        return null;
    }

    public long appendEntry(BinaryEntry key, BinaryEntry value) {
        ByteBuffer frame = frames.get(currFrameIndex);
        int requiredSpace = key.len + value.len + ENTRY_HEADER_SIZE;
        if (nextOff + requiredSpace >= frameSize) {
            // Entry doesn't fit on frame, allocate a new one.
            if (requiredSpace > frameSize) {
                throw new IllegalStateException("Key and value greater than framesize.");
            }
            frames.add(ByteBuffer.allocate(frameSize));
            currFrameIndex++;
            nextOff = 0;
            frame = frames.get(currFrameIndex);
        }
        writeEntryHeader(frame, nextOff, key.len, value.len, NULL_PTR);
        System.arraycopy(key.buf, key.off, frame.array(), nextOff + ENTRY_HEADER_SIZE, key.len);
        System.arraycopy(value.buf, value.off, frame.array(), nextOff + ENTRY_HEADER_SIZE + key.len, value.len);
        long entryPtr = getEntryPtr(currFrameIndex, nextOff);
        nextOff += requiredSpace;
        size++;
        return entryPtr;
    }

    private void writeEntryHeader(ByteBuffer frame, int targetOff, int keyLen, int valLen, long ptr) {
        frame.putShort(targetOff, (short) keyLen);
        frame.putShort(targetOff + SLOT_SIZE, (short) valLen);
        frame.putLong(targetOff + 2 * SLOT_SIZE, ptr);
    }

    private long getEntryPtr(int frameIndex, int frameOff) {
        return (((long) frameIndex) << 32) + frameOff;
    }

    private int getFrameIndex(long ptr) {
        return (int) ((ptr >> 32) & 0xffffffff);
    }

    private int getFrameOffset(long ptr) {
        return (int) (ptr & 0xffffffff);
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
    }

    public Iterator<Pair<BinaryEntry, BinaryEntry>> iterator() {
        return new BinaryHashMapIterator();
    }

    public class BinaryHashMapIterator implements Iterator<Pair<BinaryEntry, BinaryEntry>> {
        private final Pair<BinaryEntry, BinaryEntry> val = new Pair<BinaryEntry, BinaryEntry>(new BinaryEntry(),
                new BinaryEntry());
        private int listHeadIndex;
        private ByteBuffer frame;
        private int frameIndex;
        private int frameOff;

        public BinaryHashMapIterator() {
            listHeadIndex = 0;
            frame = null;
            frameIndex = -1;
            frameOff = -1;
        }

        @Override
        public boolean hasNext() {
            if (frame != null) {
                long nextPtr = frame.getLong(frameOff + 2 * SLOT_SIZE);
                if (nextPtr == NULL_PTR) {
                    // End of current list.
                    listHeadIndex++;
                    return nextListHead();
                } else {
                    // Follow pointer.
                    setValue(nextPtr);
                    return true;
                }
            }
            return nextListHead();
        }

        private boolean nextListHead() {
            // Position to first non-null list-head pointer.
            while (listHeadIndex < listHeads.length && listHeads[listHeadIndex] == NULL_PTR) {
                listHeadIndex++;
            }
            if (listHeadIndex < listHeads.length) {
                // Positioned to first non-null list head.
                setValue(listHeads[listHeadIndex]);
                return true;
            } else {
                // No more lists.
                frame = null;
                return false;
            }
        }

        private void setValue(long ptr) {
            frameIndex = getFrameIndex(ptr);
            frameOff = getFrameOffset(ptr);
            frame = frames.get(frameIndex);
            int entryKeyOff = frameOff + ENTRY_HEADER_SIZE;
            int entryKeyLen = frame.getShort(frameOff);
            int entryValOff = frameOff + ENTRY_HEADER_SIZE + entryKeyLen;
            int entryValLen = frame.getShort(frameOff + SLOT_SIZE);
            val.first.set(frame.array(), entryKeyOff, entryKeyLen);
            val.second.set(frame.array(), entryValOff, entryValLen);
        }

        @Override
        public Pair<BinaryEntry, BinaryEntry> next() {
            return val;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Remove not implemented");
        }
    }
}
