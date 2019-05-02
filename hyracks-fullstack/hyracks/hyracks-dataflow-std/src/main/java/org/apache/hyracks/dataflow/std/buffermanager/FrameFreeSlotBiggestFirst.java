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

package org.apache.hyracks.dataflow.std.buffermanager;

import org.apache.hyracks.dataflow.std.structures.IResetableComparable;
import org.apache.hyracks.dataflow.std.structures.IResetableComparableFactory;
import org.apache.hyracks.dataflow.std.structures.MaxHeap;

class FrameFreeSlotBiggestFirst implements IFrameFreeSlotPolicy {
    private static final int INVALID = -1;
    private static final int INITIAL_FRAME_NUM = 10;

    protected class SpaceEntryFactory implements IResetableComparableFactory {
        @Override
        public IResetableComparable createResetableComparable() {
            return new SpaceEntry();
        }
    }

    protected class SpaceEntry implements IResetableComparable<SpaceEntry> {
        private int space;
        private int id;

        SpaceEntry() {
            space = INVALID;
            id = INVALID;
        }

        @Override
        public int compareTo(SpaceEntry o) {
            if (o.space != space) {
                if (o.space == INVALID) {
                    return 1;
                }
                if (space == INVALID) {
                    return -1;
                }
                return space < o.space ? -1 : 1;
            }
            return 0;
        }

        @Override
        public void reset(SpaceEntry other) {
            space = other.space;
            id = other.id;
        }

        void reset(int space, int id) {
            this.space = space;
            this.id = id;
        }
    }

    private MaxHeap heap;
    private SpaceEntry tempEntry;

    public FrameFreeSlotBiggestFirst(int initialCapacity) {
        heap = new MaxHeap(new SpaceEntryFactory(), initialCapacity);
        tempEntry = new SpaceEntry();
    }

    public FrameFreeSlotBiggestFirst() {
        this(INITIAL_FRAME_NUM);
    }

    @Override
    public int popBestFit(int tobeInsertedSize) {
        if (!heap.isEmpty()) {
            heap.peekMax(tempEntry);
            if (tempEntry.space >= tobeInsertedSize) {
                heap.getMax(tempEntry);
                return tempEntry.id;
            }
        }
        return -1;
    }

    @Override
    public void pushNewFrame(int frameID, int freeSpace) {
        tempEntry.reset(freeSpace, frameID);
        heap.insert(tempEntry);
    }

    @Override
    public void reset() {
        // TODO(ali): fix to not release resources
        heap.reset();
    }

    @Override
    public void close() {
        heap.reset();
    }
}
