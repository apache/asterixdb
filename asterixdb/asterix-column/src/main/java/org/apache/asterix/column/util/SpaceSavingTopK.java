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
package org.apache.asterix.column.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.data.std.api.IValueReference;

/**
 * Finds the most frequent keys (Space-Saving algorithm) using a fixed number of slots, however many distinct keys
 * arrive. Used to find promotion candidates in a collapsed node without counting every key.
 * <p>
 * {@link Entry#count()} may be too high; {@code count() - error()} is a guaranteed lower bound.
 * <p>
 * Slots are parallel arrays scanned linearly, which is as fast as hashing at a few dozen slots and allocates nothing
 * when a new key evicts an old one. {@link Entry#key()} is valid only until the next {@link #add}; copy it to keep it.
 */
public final class SpaceSavingTopK {
    public static final class Entry {
        private final byte[] key;
        private final long count;
        private final long error;
        /** Type of the latest non-null value seen for this key; null if none yet. */
        private final ATypeTag typeTag;

        private Entry(byte[] key, long count, long error, ATypeTag typeTag) {
            this.key = key;
            this.count = count;
            this.error = error;
            this.typeTag = typeTag;
        }

        public byte[] key() {
            return key;
        }

        public long count() {
            return count;
        }

        public long error() {
            return error;
        }

        public ATypeTag typeTag() {
            return typeTag;
        }
    }

    private final int[] hashes;
    private final long[] counts;
    private final long[] errors;
    /** Each array is exactly the key's length, since {@link Entry#key()} returns the whole array. */
    private final byte[][] keys;
    private final ATypeTag[] typeTags;
    /** Scratch for {@link #keysAtLeast}: slots already returned. */
    private final boolean[] picked;
    private int size;

    public SpaceSavingTopK(int capacity) {
        picked = new boolean[capacity];
        hashes = new int[capacity];
        counts = new long[capacity];
        errors = new long[capacity];
        keys = new byte[capacity][];
        typeTags = new ATypeTag[capacity];
    }

    /**
     * Counts {@code key} and, if {@code typeTag} is non-null, records it as the key's type. The latest type wins: a key
     * with mixed types becomes a union column anyway.
     */
    public void add(IValueReference key, ATypeTag typeTag) {
        byte[] bytes = key.getByteArray();
        int offset = key.getStartOffset();
        int length = key.getLength();
        int hash = 1;
        for (int i = offset; i < offset + length; i++) {
            hash = 31 * hash + bytes[i];
        }
        for (int i = 0; i < size; i++) {
            if (hashes[i] == hash && Arrays.equals(keys[i], 0, keys[i].length, bytes, offset, offset + length)) {
                counts[i]++;
                if (typeTag != null) {
                    typeTags[i] = typeTag;
                }
                return;
            }
        }
        int slot;
        long inherited;
        if (size < hashes.length) {
            slot = size++;
            inherited = 0;
        } else {
            // Space-Saving: replace the smallest entry; its count becomes the new key's error.
            slot = 0;
            for (int i = 1; i < size; i++) {
                if (counts[i] < counts[slot]) {
                    slot = i;
                }
            }
            inherited = counts[slot];
        }
        if (keys[slot] == null || keys[slot].length != length) {
            keys[slot] = new byte[length];
        }
        System.arraycopy(bytes, offset, keys[slot], 0, length);
        hashes[slot] = hash;
        counts[slot] = inherited + 1;
        errors[slot] = inherited;
        typeTags[slot] = typeTag;
    }

    /** Removes {@code key} if present, moving the last entry into its slot. */
    public void remove(IValueReference key) {
        byte[] bytes = key.getByteArray();
        int offset = key.getStartOffset();
        int length = key.getLength();
        for (int i = 0; i < size; i++) {
            if (Arrays.equals(keys[i], 0, keys[i].length, bytes, offset, offset + length)) {
                int last = --size;
                byte[] removed = keys[i];
                keys[i] = keys[last];
                keys[last] = removed;
                hashes[i] = hashes[last];
                counts[i] = counts[last];
                errors[i] = errors[last];
                typeTags[i] = typeTags[last];
                return;
            }
        }
    }

    /** Empties the counter; key arrays are kept for reuse. */
    public void reset() {
        size = 0;
    }

    /**
     * Copies of the keys whose lower bound ({@code count() - error()}) reaches {@code threshold} and that have a
     * non-null type, most frequent first. Allocates nothing when none qualify, the usual case.
     */
    public List<byte[]> keysAtLeast(double threshold) {
        // Counts are integers: reaching threshold means exceeding this.
        long belowThreshold = (long) Math.ceil(threshold) - 1;
        List<byte[]> keysFound = Collections.emptyList();
        Arrays.fill(picked, 0, size, false);
        while (true) {
            int best = -1;
            long bestBound = belowThreshold;
            for (int i = 0; i < size; i++) {
                long bound = counts[i] - errors[i];
                if (!picked[i] && typeTags[i] != null && bound > bestBound) {
                    best = i;
                    bestBound = bound;
                }
            }
            if (best < 0) {
                return keysFound;
            }
            picked[best] = true;
            if (keysFound.isEmpty()) {
                keysFound = new ArrayList<>();
            }
            keysFound.add(keys[best].clone());
        }
    }

    /** All entries, for tests and debugging. */
    public List<Entry> entries() {
        List<Entry> entries = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            entries.add(new Entry(keys[i], counts[i], errors[i], typeTags[i]));
        }
        return entries;
    }
}
