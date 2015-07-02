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

package org.apache.hyracks.dataflow.std.structures;

public class MinHeap extends AbstractHeap implements IMinHeap<IResetableComparable> {

    public MinHeap(IResetableComparableFactory factory, int capacity) {
        super(factory, capacity);
    }

    @Override
    protected void bubbleUp(int i) {
        int pid = getParentId(i);
        if (pid != NOT_EXIST && compareTo(pid, i) > 0) {
            swap(pid, i);
            bubbleUp(pid);
        }
    }

    @Override
    protected void trickleDown(int i) {
        int minChild = getMinChild(i);
        if (minChild != NOT_EXIST && compareTo(i, minChild) > 0) {
            swap(minChild, i);
            trickleDown(minChild);
        }
    }

    @Override
    public void getMin(IResetableComparable result) {
        result.reset(entries[0]);
        numEntry--;
        if (numEntry > 0) {
            entries[0].reset(entries[numEntry]);
            trickleDown(0);
        }
    }

    @Override
    public void peekMin(IResetableComparable result) {
        result.reset(entries[0]);
    }

    @Override
    public void replaceMin(IResetableComparable newElement) {
        entries[0].reset(newElement);
        trickleDown(0);
    }
}
