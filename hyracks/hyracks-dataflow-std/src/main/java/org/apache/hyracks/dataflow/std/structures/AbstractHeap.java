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

import java.util.Arrays;

import org.apache.hyracks.util.MathUtil;

public abstract class AbstractHeap implements IHeap<IResetableComparable> {
    protected static final int NOT_EXIST = -1;
    protected static final int MAX_INITIAL_CAPACITY = 1024;
    protected IResetableComparable[] entries;
    protected IResetableComparable tempEntry;
    protected IResetableComparableFactory factory;
    protected int numEntry;

    public AbstractHeap(IResetableComparableFactory factory, int capacity) {
        capacity = Math.min(MAX_INITIAL_CAPACITY, Math.max(1, capacity));
        this.entries = new IResetableComparable[capacity];
        this.numEntry = 0;
        this.tempEntry = factory.createResetableComparable();
        this.factory = factory;
    }

    @Override
    public void insert(IResetableComparable element) {
        if (numEntry >= entries.length) {
            entries = Arrays.copyOf(entries, entries.length * 2);
        }
        if (entries[numEntry] == null) {
            entries[numEntry] = factory.createResetableComparable();
        }
        entries[numEntry++].reset(element);
        bubbleUp(numEntry - 1);
    }

    protected abstract void bubbleUp(int i);

    protected abstract void trickleDown(int i);

    protected void swap(int cid, int pid) {
        tempEntry.reset(entries[cid]);
        entries[cid].reset(entries[pid]);
        entries[pid].reset(tempEntry);
    }

    protected int compareTo(int i, int j) {
        return entries[i].compareTo(entries[j]);
    }

    @Override
    public boolean isEmpty() {
        return numEntry == 0;
    }

    @Override
    public void reset() {
        Arrays.fill(entries, null);
        numEntry = 0;
    }

    /**
     * By getting the entries it can manipulate the entries which may violate the Heap property.
     * Use with care.
     *
     * @return
     */
    @Deprecated
    public IResetableComparable[] getEntries() {
        return entries;
    }

    @Override
    public int getNumEntries() {
        return numEntry;
    }

    protected int getLevel(int cid) {
        return MathUtil.log2Floor(cid + 1);
    }

    static int getParentId(int cid) {
        return cid < 1 ? NOT_EXIST : (cid - 1) / 2;
    }

    static int getLeftChild(int id, int numEntry) {
        int cid = id * 2 + 1;
        return cid >= numEntry ? NOT_EXIST : cid;
    }

    protected int getLeftChild(int id) {
        return getLeftChild(id, numEntry);
    }

    static int getRightChild(int id, int numEntry) {
        int cid = id * 2 + 2;
        return cid >= numEntry ? NOT_EXIST : cid;
    }

    protected int getRightChild(int id) {
        return getRightChild(id, numEntry);
    }

    protected int getGrandParentId(int id) {
        int pid = getParentId(id);
        return pid == NOT_EXIST ? NOT_EXIST : getParentId(pid);
    }

    protected boolean isDirectChild(int id, int childId) {
        return id == getParentId(childId);
    }

    protected int getMinChild(int id) {
        int min = NOT_EXIST;
        if (id != NOT_EXIST) {
            min = getLeftChild(id, numEntry);
            if (min != NOT_EXIST) {
                int rightCid = getRightChild(id, numEntry);
                if (rightCid != NOT_EXIST) {
                    min = compareTo(rightCid, min) < 0 ? rightCid : min;
                }
            }
        }
        return min;
    }

    protected int getMaxChild(int id) {
        int max = NOT_EXIST;
        if (id != NOT_EXIST) {
            max = getLeftChild(id, numEntry);
            if (max != NOT_EXIST) {
                int rightCid = getRightChild(id, numEntry);
                if (rightCid != NOT_EXIST) {
                    max = compareTo(rightCid, max) > 0 ? rightCid : max;
                }
            }
        }
        return max;
    }

}
