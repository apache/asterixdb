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

package org.apache.asterix.runtime.operators.joins.intervalindex;

import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.logging.Logger;

import org.apache.asterix.runtime.operators.joins.IntervalJoinUtil;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.std.buffermanager.IPartitionedDeletableTupleBufferManager;
import org.apache.hyracks.dataflow.std.buffermanager.ITupleAccessor;
import org.apache.hyracks.dataflow.std.structures.TuplePointer;

public class ActiveSweepManager {

    private static final Logger LOGGER = Logger.getLogger(ActiveSweepManager.class.getName());

    private final int LEFT_PARTITION;
    private final int leftKey;

    private IPartitionedDeletableTupleBufferManager bufferManager;
    private PriorityQueue<EndPointIndexItem> leftIndexQueue;
    private EndPointIndexItem leftItem = null;
    private LinkedList<TuplePointer> leftActive = new LinkedList<TuplePointer>();

    public ActiveSweepManager(IPartitionedDeletableTupleBufferManager bufferManager, int key, int partition,
            Comparator<EndPointIndexItem> endPointComparator) {
        this.bufferManager = bufferManager;
        this.leftKey = key;
        LEFT_PARTITION = partition;
        leftIndexQueue = new PriorityQueue<EndPointIndexItem>(16, endPointComparator);
    }

    public boolean addTuple(ITupleAccessor leftInputAccessor, TuplePointer tp) throws HyracksDataException {
        if (bufferManager.insertTuple(LEFT_PARTITION, leftInputAccessor, leftInputAccessor.getTupleId(), tp)) {
            EndPointIndexItem e = new EndPointIndexItem(tp, EndPointIndexItem.END_POINT,
                    IntervalJoinUtil.getIntervalEnd(leftInputAccessor, leftInputAccessor.getTupleId(), leftKey));
            leftIndexQueue.add(e);
            leftActive.add(tp);
            leftItem = leftIndexQueue.peek();
            return true;
        }
        return false;
    }

    public void removeTop() throws HyracksDataException {
        // Remove from active.
        bufferManager.deleteTuple(LEFT_PARTITION, leftItem.getTuplePointer());
        leftActive.remove(leftItem.getTuplePointer());
        leftIndexQueue.remove(leftItem);
        leftItem = leftIndexQueue.peek();
    }

    public long getTopPoint() {
        return leftItem.getPoint();
    }

    public List<TuplePointer> getActiveList() {
        return leftActive;
    }

    public boolean isEmpty() {
        return leftIndexQueue.isEmpty();
    }
    public boolean hasRecords() {
        return !leftIndexQueue.isEmpty();
    }

    public void clear() throws HyracksDataException {
        for (TuplePointer leftTp : leftActive) {
            bufferManager.deleteTuple(LEFT_PARTITION, leftTp);
        }
        leftIndexQueue.clear();
        leftActive.clear();
        leftItem = null;
        bufferManager.clearPartition(LEFT_PARTITION);
    }
}