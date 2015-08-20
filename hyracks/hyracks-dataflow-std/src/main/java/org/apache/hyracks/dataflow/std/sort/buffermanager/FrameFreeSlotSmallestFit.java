/*
 * Copyright 2009-2013 by The Regents of the University of California
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package edu.uci.ics.hyracks.dataflow.std.sort.buffermanager;

import java.util.LinkedList;
import java.util.Map;
import java.util.TreeMap;

public class FrameFreeSlotSmallestFit implements IFrameFreeSlotPolicy {

    private TreeMap<Integer, LinkedList<Integer>> freeSpaceIndex;

    public FrameFreeSlotSmallestFit() {
        freeSpaceIndex = new TreeMap<>();
    }

    @Override
    public int popBestFit(int tobeInsertedSize) {
        Map.Entry<Integer, LinkedList<Integer>> entry = freeSpaceIndex.ceilingEntry(tobeInsertedSize);
        if (entry == null) {
            return -1;
        }
        int id = entry.getValue().removeFirst();
        if (entry.getValue().isEmpty()) {
            freeSpaceIndex.remove(entry.getKey());
        }
        return id;
    }

    @Override
    public void pushNewFrame(int frameID, int freeSpace) {
        Map.Entry<Integer, LinkedList<Integer>> entry = freeSpaceIndex.ceilingEntry(freeSpace);
        if (entry == null || entry.getKey() != freeSpace) {
            LinkedList<Integer> linkedList = new LinkedList<>();
            linkedList.add(frameID);
            freeSpaceIndex.put(freeSpace, linkedList);
        } else {
            entry.getValue().add(frameID);
        }
    }

    @Override
    public void reset() {
        freeSpaceIndex.clear();
    }
}
