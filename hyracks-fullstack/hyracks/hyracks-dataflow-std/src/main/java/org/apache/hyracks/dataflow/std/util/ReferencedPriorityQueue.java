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
package org.apache.hyracks.dataflow.std.util;

import java.io.IOException;
import java.util.BitSet;
import java.util.Comparator;

import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputer;

public class ReferencedPriorityQueue {
    private final ReferenceEntry entries[];
    private final int size;
    private final BitSet runAvail;
    private int nItems;

    private final Comparator<ReferenceEntry> comparator;
    private final INormalizedKeyComputer nmkComputer;
    private final int[] keyFields;

    public ReferencedPriorityQueue(int initSize, Comparator<ReferenceEntry> comparator, int[] keyFields,
            INormalizedKeyComputer nmkComputer) {
        if (initSize < 1)
            throw new IllegalArgumentException();
        this.comparator = comparator;
        this.nmkComputer = nmkComputer;
        this.keyFields = keyFields;
        nItems = initSize;
        size = (initSize + 1) & 0xfffffffe;
        entries = new ReferenceEntry[size];
        runAvail = new BitSet(size);
        runAvail.set(0, initSize, true);
        for (int i = 0; i < size; i++) {
            entries[i] = new ReferenceEntry(i, null, -1, keyFields, nmkComputer);
        }
    }

    /**
     * Retrieve the top entry without removing it
     *
     * @return the top entry
     */
    public ReferenceEntry peek() {
        return entries[0];
    }

    /**
     * compare the new entry with entries within the queue, to find a spot for
     * this new entry
     *
     * @param fta
     * @return runid of this entry
     * @throws IOException
     */
    public int popAndReplace(IFrameTupleAccessor fta, int tIndex) {
        ReferenceEntry entry = entries[0];
        entry.setAccessor(fta);
        entry.setTupleIndex(tIndex, keyFields, nmkComputer);

        add(entry);
        return entry.getRunid();
    }

    /**
     * Push entry into priority queue
     *
     * @param e the new Entry
     */
    private void add(ReferenceEntry e) {
        ReferenceEntry min = entries[0];
        int slot = (size >> 1) + (min.getRunid() >> 1);

        ReferenceEntry curr = e;
        while (!runAvail.isEmpty() && slot > 0) {
            int c = 0;
            if (!runAvail.get(entries[slot].getRunid())) {
                // run of entries[slot] is exhausted, i.e. not available, curr
                // wins
                c = 1;
            } else if (entries[slot].getAccessor() != null /*
                                                            * entries[slot] is
                                                            * not MIN value
                                                            */
                    && runAvail.get(curr.getRunid() /* curr run is available */)) {

                if (curr.getAccessor() != null) {
                    c = comparator.compare(entries[slot], curr);
                } else {
                    // curr is MIN value, wins
                    c = 1;
                }
            }

            if (c <= 0) { // curr lost
                // entries[slot] swaps up
                ReferenceEntry tmp = entries[slot];
                entries[slot] = curr;
                curr = tmp;// winner to pass up
            } // else curr wins
            slot >>= 1;
        }
        // set new entries[0]
        entries[0] = curr;
    }

    /**
     * Pop is called only when a run is exhausted
     *
     * @return
     */
    public ReferenceEntry pop() {
        ReferenceEntry min = entries[0];
        runAvail.clear(min.getRunid());
        add(min);
        nItems--;
        return min;
    }

    public boolean areRunsExhausted() {
        return runAvail.isEmpty();
    }

    public int size() {
        return nItems;
    }
}
