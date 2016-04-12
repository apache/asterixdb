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

import java.util.BitSet;

public class SelectionTree {
    private int size;

    private Entry[] entries;

    private int[] losers;

    private BitSet available;

    public SelectionTree(Entry[] e) {
        size = (e.length + 1) & 0xfffffffe;
        available = new BitSet(size);
        available.set(0, e.length, true);
        losers = new int[size];
        entries = e;
        for (int i = 0; i < size; ++i) {
            losers[i] = -1;
        }
        for (int i = 0; i < size; ++i) {
            int slot = (size + i) >> 1;

            if (i < entries.length) {
                available.set(i, entries[i].advance());
            }
            int currIdx = i;
            while (slot > 0) {
                int cmp = 0;
                if (losers[slot] < 0 || currIdx < 0) {
                    cmp = losers[slot] < 0 ? -1 : 1;
                } else if (!available.get(losers[slot])) {
                    cmp = 1;
                } else if (available.get(currIdx)) {
                    if (currIdx <= i) {
                        cmp = entries[losers[slot]].compareTo(entries[currIdx]);
                    } else {
                        cmp = 1;
                    }
                }

                if (cmp <= 0) {
                    int tmp = losers[slot];
                    losers[slot] = currIdx;
                    currIdx = tmp;
                }
                slot >>= 1;
            }
            losers[0] = currIdx;
        }
    }

    public Entry peek() {
        if (entries.length == 0) {
            return null;
        }
        return entries[losers[0]];
    }

    public void pop() {
        int winner = losers[0];
        int slot = (size + winner) >> 1;

        boolean avail = entries[winner].advance();
        if (!avail) {
            entries[winner] = null;
        }
        available.set(winner, avail);
        int currIdx = winner;
        while (!available.isEmpty() && slot > 0) {
            int cmp = 0;
            if (!available.get(losers[slot])) {
                cmp = 1;
            } else if (available.get(currIdx)) {
                cmp = entries[losers[slot]].compareTo(entries[currIdx]);
            }

            if (cmp <= 0) {
                int tmp = losers[slot];
                losers[slot] = currIdx;
                currIdx = tmp;
            }
            slot >>= 1;
        }
        losers[0] = currIdx;
    }

    public interface Entry extends Comparable<Entry> {
        public abstract boolean advance();
    }
}
