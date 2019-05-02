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

import java.util.LinkedList;
import java.util.Map;
import java.util.TreeMap;

class FrameFreeSlotSmallestFit implements IFrameFreeSlotPolicy {

    private TreeMap<Integer, LinkedList<Integer>> freeSpaceIndex;

    FrameFreeSlotSmallestFit() {
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
        // TODO(ali): fix to not release resources
        freeSpaceIndex.clear();
    }

    @Override
    public void close() {
        freeSpaceIndex.clear();
    }
}
