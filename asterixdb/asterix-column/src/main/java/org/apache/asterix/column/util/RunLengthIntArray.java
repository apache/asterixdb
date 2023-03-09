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

import java.util.Arrays;

import it.unimi.dsi.fastutil.ints.IntArrayList;

/**
 * Run-length integer array is to be used for storing repetitive integer values. This is intended for
 * storing a large number of repeated integers (~1000s). It is not recommended for storing smaller number of integers.
 * This structure maintains two arrays:
 * - blockValues: stores the array values
 * - blockCounts: stores the counts of values in <code>blockValues</code> in a monotonic fashion
 * <pr>
 * Example:
 * Original Array: [1,1,1,1,1,1,2,2,2,1,1,1]
 * blockValues: [1,2,1]
 * blockCounts: [6,10,13]
 */
public final class RunLengthIntArray {
    private final IntArrayList blockValues;
    private int[] blockCounts;
    private int lastSeen;
    private int size;

    public RunLengthIntArray() {
        blockValues = new IntArrayList();
        blockCounts = new int[32];
        reset();
    }

    public void reset() {
        blockValues.clear();
        lastSeen = -1;
        size = 0;
    }

    public void add(int value) {
        if (size == 0 || value != lastSeen) {
            lastSeen = value;
            newBlock();
            blockValues.add(value);
        }
        blockCounts[blockValues.size() - 1]++;
        size++;
    }

    public void add(int value, int count) {
        if (count == 0) {
            return;
        }
        if (size == 0 || value != lastSeen) {
            lastSeen = value;
            newBlock();
            blockValues.add(value);
        }
        blockCounts[blockValues.size() - 1] += count;
        size += count;
    }

    public int getSize() {
        return size;
    }

    public int getNumberOfBlocks() {
        return blockValues.size();
    }

    public int getBlockValue(int blockIndex) {
        return blockValues.getInt(blockIndex);
    }

    public int getBlockSize(int blockIndex) {
        if (blockIndex == 0) {
            return blockCounts[blockIndex];
        }
        return blockCounts[blockIndex] - blockCounts[blockIndex - 1];
    }

    public int getBlockSize(int blockIndex, int startIndex) {
        return blockCounts[blockIndex] - startIndex;
    }

    public int getBlockIndex(int startIndex) {
        if (startIndex >= size) {
            throw new IndexOutOfBoundsException("startIndex: " + startIndex + " >= size:" + size);
        }
        int index = Arrays.binarySearch(blockCounts, 0, blockValues.size(), startIndex);
        if (index < 0) {
            index = Math.abs(index) - 1;
        }
        return index;
    }

    public void add(RunLengthIntArray other, int startIndex) {
        if (startIndex >= other.size) {
            throw new IndexOutOfBoundsException("startIndex: " + startIndex + " >= other size:" + size);
        }
        //First, handle the first block as startIndex might be at the middle of a block
        //Get which block that startIndex resides
        int otherBlockIndex = other.getBlockIndex(startIndex);
        //Get the remaining of the first block starting from startIndex
        int otherBlockSizeRemaining = other.getBlockSize(otherBlockIndex, startIndex);
        //Batch add all the remaining values
        add(other.getBlockValue(otherBlockIndex), otherBlockSizeRemaining);

        //Add other blocks as batches
        for (int i = otherBlockIndex + 1; i < other.getNumberOfBlocks(); i++) {
            add(other.getBlockValue(i), other.getBlockSize(i));
        }
    }

    private void newBlock() {
        int newBlockIndex = blockValues.size();
        if (newBlockIndex == blockCounts.length) {
            int[] newRepCount = new int[blockCounts.length * 2];
            System.arraycopy(blockCounts, 0, newRepCount, 0, blockCounts.length);
            blockCounts = newRepCount;
        }
        if (newBlockIndex > 0) {
            /*
             * To easily compute where the actual block resides, the block counts are always increasing.
             * For example:
             * - Let blockCounts = [5, 6, 13] and blockValues = [1, 0, 1]
             * - The block sizes are 5, 1, and 7 respectively
             * - Let say that we want to know what is the value at index 11 by calling getValue(11)
             * - by searching blockCounts, we know it is at the block with index 2
             * - Then the value is 1
             */
            blockCounts[newBlockIndex] = blockCounts[newBlockIndex - 1];
        } else {
            blockCounts[0] = 0;
        }
    }

    @Override
    public String toString() {
        if (size == 0) {
            return "[]";
        }
        StringBuilder builder = new StringBuilder();
        int i = 0;
        builder.append("size: ");
        builder.append(getSize());
        builder.append(" [");
        for (; i < getNumberOfBlocks() - 1; i++) {
            appendBlockInfo(i, builder);
            builder.append(',');
        }
        appendBlockInfo(i, builder);
        builder.append(']');
        return builder.toString();
    }

    private void appendBlockInfo(int blockIndex, StringBuilder builder) {
        builder.append('(');
        builder.append(getBlockValue(blockIndex));
        builder.append(',');
        builder.append(getBlockSize(blockIndex));
        builder.append(')');
    }
}
