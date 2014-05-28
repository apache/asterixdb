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
package edu.uci.ics.pregelix.dataflow.std.sort;

import java.io.IOException;

import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.pregelix.dataflow.std.sort.RunMergingFrameReader.EntryComparator;
import edu.uci.ics.pregelix.dataflow.std.util.ReferenceEntry;

public class ReferencedPriorityQueue {
    private final int frameSize;
    private final RecordDescriptor recordDescriptor;
    private final ReferenceEntry entries[];
    private final int size;
    private int nItems;

    private final EntryComparator comparator;
    private final RawNormalizedKeyComputer nmkComputer = new RawNormalizedKeyComputer();
    private final int[] keyFields;

    public ReferencedPriorityQueue(int frameSize, RecordDescriptor recordDescriptor, int initSize,
            EntryComparator comparator, int[] keyFields) {
        this.frameSize = frameSize;
        this.recordDescriptor = recordDescriptor;
        if (initSize < 1)
            throw new IllegalArgumentException();
        this.comparator = comparator;
        this.keyFields = keyFields;
        nItems = initSize;
        size = (initSize + 1) & 0xfffffffe;
        entries = new ReferenceEntry[size];
        for (int i = 0; i < size; i++) {
            entries[i] = new ReferenceEntry(i, null, -1, keyFields, nmkComputer);
        }
        for (int i = initSize; i < size; i++) {
            entries[i].setExhausted();
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
     * @param entry
     * @return runid of this entry
     * @throws IOException
     */
    public int popAndReplace(FrameTupleAccessor fta, int tIndex) {
        ReferenceEntry entry = entries[0];
        if (entry.getAccessor() == null) {
            entry.setAccessor(new FrameTupleAccessor(frameSize, recordDescriptor));
        }
        entry.getAccessor().reset(fta.getBuffer());
        entry.setTupleIndex(tIndex, keyFields, nmkComputer);

        add(entry);
        return entry.getRunid();
    }

    /**
     * Push entry into priority queue
     * 
     * @param e
     *            the new Entry
     */
    private void add(ReferenceEntry e) {
        ReferenceEntry min = entries[0];
        int slot = (size >> 1) + (min.getRunid() >> 1);

        ReferenceEntry curr = e;
        while (nItems > 0 && slot > 0) {
            int c = 0;
            if (entries[slot].isExhausted()) {
                // run of entries[slot] is exhausted, i.e. not available, curr
                // wins
                c = 1;
            } else if (entries[slot].getAccessor() != null /*
                                                            * entries[slot] is
                                                            * not MIN value
                                                            */
                    && !curr.isExhausted() /* curr run is available */) {

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
            }// else curr wins
            slot = slot >> 1;
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
        min.setExhausted();
        add(min);
        nItems--;
        return min;
    }

    public boolean areRunsExhausted() {
        return nItems <= 0;
    }

    public int size() {
        return nItems;
    }
}