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

public class MinMaxHeap extends AbstractHeap implements IMinMaxHeap<IResetableComparable> {

    public MinMaxHeap(IResetableComparableFactory factory, int capacity) {
        super(factory, capacity);
    }

    @Override
    protected void bubbleUp(int cid) {
        int pid = getParentId(cid);
        if (isAtMinLevel(cid)) {
            if (pid != NOT_EXIST && entries[pid].compareTo(entries[cid]) < 0) {
                swap(cid, pid);
                bubbleUpMax(pid);
            } else {
                bubbleUpMin(cid);
            }
        } else { // isAtMaxLevel
            if (pid != NOT_EXIST && entries[pid].compareTo(entries[cid]) > 0) {
                swap(cid, pid);
                bubbleUpMin(pid);
            } else {
                bubbleUpMax(cid);
            }
        }
    }

    private void bubbleUpMin(int id) {
        int gp = getGrandParentId(id);
        if (gp != NOT_EXIST && entries[gp].compareTo(entries[id]) > 0) {
            swap(gp, id);
            bubbleUpMin(gp);
        }
    }

    private void bubbleUpMax(int id) {
        int gp = getGrandParentId(id);
        if (gp != NOT_EXIST && entries[gp].compareTo(entries[id]) < 0) {
            swap(gp, id);
            bubbleUpMax(gp);
        }
    }

    private boolean isAtMinLevel(int cid) {
        return getLevel(cid) % 2 == 0;
    }

    /**
     * Make sure to check the {@link #isEmpty()} before calling this function.
     *
     * @param result
     */
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
    public void getMax(IResetableComparable result) {
        int max = getMaxChild(0);
        if (max == NOT_EXIST) {
            getMin(result);
            return;
        }
        result.reset(entries[max]);
        numEntry--;
        if (numEntry > max) {
            entries[max].reset(entries[numEntry]);
            trickleDown(max);
        }
    }

    @Override
    protected void trickleDown(int id) {
        if (isAtMinLevel(id)) {
            trickleDownMin(id);
        } else {
            trickleDownMax(id);
        }
    }

    private void trickleDownMax(int id) {
        int maxId = getMaxOfDescendents(id);
        if (maxId == NOT_EXIST) {
            return;
        }
        if (isDirectChild(id, maxId)) {
            if (entries[id].compareTo(entries[maxId]) < 0) {
                swap(id, maxId);
            }
        } else {
            if (entries[id].compareTo(entries[maxId]) < 0) {
                swap(id, maxId);
                int pid = getParentId(maxId);
                if (entries[maxId].compareTo(entries[pid]) < 0) {
                    swap(pid, maxId);
                }
                trickleDownMax(maxId);
            }
        }
    }

    private void trickleDownMin(int id) {
        int minId = getMinOfDescendents(id);
        if (minId == NOT_EXIST) {
            return;
        }
        if (isDirectChild(id, minId)) {
            if (entries[id].compareTo(entries[minId]) > 0) {
                swap(id, minId);
            }
        } else { // is grand child
            if (entries[id].compareTo(entries[minId]) > 0) {
                swap(id, minId);
                int pid = getParentId(minId);
                if (entries[minId].compareTo(entries[pid]) > 0) {
                    swap(pid, minId);
                }
                trickleDownMin(minId);
            }
        }
    }

    private int getMaxOfDescendents(int id) {
        int max = getMaxChild(id);
        if (max != NOT_EXIST) {
            int leftMax = getMaxChild(getLeftChild(id));
            if (leftMax != NOT_EXIST) {
                max = entries[leftMax].compareTo(entries[max]) > 0 ? leftMax : max;
                int rightMax = getMaxChild(getRightChild(id));
                if (rightMax != NOT_EXIST) {
                    max = entries[rightMax].compareTo(entries[max]) > 0 ? rightMax : max;
                }
            }
        }
        return max;
    }

    private int getMinOfDescendents(int id) {
        int min = getMinChild(id);
        if (min != NOT_EXIST) {
            int leftMin = getMinChild(getLeftChild(id));
            if (leftMin != NOT_EXIST) {
                min = entries[leftMin].compareTo(entries[min]) < 0 ? leftMin : min;
                int rightMin = getMinChild(getRightChild(id));
                if (rightMin != NOT_EXIST) {
                    min = entries[rightMin].compareTo(entries[min]) < 0 ? rightMin : min;
                }
            }
        }
        return min;
    }

    public boolean isEmpty() {
        return numEntry == 0;
    }

    /**
     * Make sure to call the {@link #isEmpty()} before calling this function
     *
     * @param result is the object that will eventually contain minimum entry
     */
    @Override
    public void peekMin(IResetableComparable result) {
        result.reset(entries[0]);
    }

    @Override
    public void peekMax(IResetableComparable result) {
        int maxChild = getMaxChild(0);
        if (maxChild == NOT_EXIST) {
            peekMin(result);
            return;
        }
        result.reset(entries[maxChild]);
    }

    @Override
    public void replaceMin(IResetableComparable newElement) {
        entries[0].reset(newElement);
        trickleDown(0);
    }

    @Override
    public void replaceMax(IResetableComparable newElement) {
        int maxChild = getMaxChild(0);
        if (maxChild == NOT_EXIST) {
            replaceMin(newElement);
            return;
        }
        entries[maxChild].reset(newElement);
        bubbleUp(maxChild);
        trickleDown(maxChild);
    }

}
