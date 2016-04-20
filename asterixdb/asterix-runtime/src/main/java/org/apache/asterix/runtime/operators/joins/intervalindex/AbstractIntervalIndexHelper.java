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
import java.util.PriorityQueue;
import java.util.logging.Logger;

import org.apache.asterix.runtime.operators.joins.IntervalJoinUtil;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.std.buffermanager.IPartitionedDeletableTupleBufferManager;
import org.apache.hyracks.dataflow.std.buffermanager.IPartitionedTupleBufferManager;
import org.apache.hyracks.dataflow.std.buffermanager.ITupleAccessor;
import org.apache.hyracks.dataflow.std.buffermanager.ITuplePointerAccessor;
import org.apache.hyracks.dataflow.std.structures.TuplePointer;

public abstract class AbstractIntervalIndexHelper implements IIntervalIndexHelper {

    private final int key;
    private IPartitionedTupleBufferManager bufferManager;
    private PriorityQueue<EndPointIndexItem> indexQueue;
    private long sweep;
    private final byte point;
    private boolean continueBuldingIndex = true;
    private RecordDescriptor recordDescriptor;
    private int partition;

    private static final Logger LOGGER = Logger.getLogger(AbstractIntervalIndexHelper.class.getName());

    public AbstractIntervalIndexHelper(IHyracksTaskContext ctx, RecordDescriptor rd, int key,
            IPartitionedDeletableTupleBufferManager bufferManager, Comparator<EndPointIndexItem> endPointComparator, byte point,
            int partition) {
        this.key = key;
        this.recordDescriptor = rd;
        this.bufferManager = bufferManager;
        indexQueue = new PriorityQueue<EndPointIndexItem>(16, endPointComparator);
        this.point = point;
        this.partition = partition;
    }

    public boolean addTupleToIndex(IFrameTupleAccessor accessor, int i, TuplePointer tuplePointer)
            throws HyracksDataException {
        if (bufferManager.insertTuple(getMemoryPartition(), accessor, i, tuplePointer)) {
            sweep = (point == EndPointIndexItem.START_POINT ? IntervalJoinUtil.getIntervalStart(accessor, i, key)
                    : IntervalJoinUtil.getIntervalEnd(accessor, i, key));
            EndPointIndexItem s = new EndPointIndexItem(tuplePointer, EndPointIndexItem.START_POINT,
                    IntervalJoinUtil.getIntervalStart(accessor, i, key));
            indexQueue.add(s);
            EndPointIndexItem e = new EndPointIndexItem(tuplePointer, EndPointIndexItem.END_POINT,
                    IntervalJoinUtil.getIntervalEnd(accessor, i, key));
            indexQueue.add(e);
            return true;
        } else {
            return false;
        }
    }

    public boolean addTupleToIndex(ITupleAccessor accessor, TuplePointer tuplePointer) throws HyracksDataException {
        if (bufferManager.insertTuple(getMemoryPartition(), accessor, accessor.getTupleId(), tuplePointer)) {
            sweep = (point == EndPointIndexItem.START_POINT
                    ? IntervalJoinUtil.getIntervalStart(accessor, accessor.getTupleId(), key)
                    : IntervalJoinUtil.getIntervalEnd(accessor, accessor.getTupleId(), key));
            EndPointIndexItem s = new EndPointIndexItem(tuplePointer, EndPointIndexItem.START_POINT,
                    IntervalJoinUtil.getIntervalStart(accessor, accessor.getTupleId(), key));
            indexQueue.add(s);
            EndPointIndexItem e = new EndPointIndexItem(tuplePointer, EndPointIndexItem.END_POINT,
                    IntervalJoinUtil.getIntervalEnd(accessor, accessor.getTupleId(), key));
            indexQueue.add(e);
            return true;
        } else {
            return false;
        }
    }

    public boolean addTupleEndToIndex(ITupleAccessor accessor, TuplePointer tuplePointer) throws HyracksDataException {
        if (bufferManager.insertTuple(getMemoryPartition(), accessor, accessor.getTupleId(), tuplePointer)) {
            EndPointIndexItem e = new EndPointIndexItem(tuplePointer, EndPointIndexItem.END_POINT,
                    IntervalJoinUtil.getIntervalEnd(accessor, accessor.getTupleId(), key));
            indexQueue.add(e);
            return true;
        } else {
            return false;
        }
    }

    public long getSweep() {
        return sweep;
    }

    public ITuplePointerAccessor createTuplePointerAccessor() {
        return bufferManager.getTuplePointerAccessor(recordDescriptor);
    }

    public EndPointIndexItem next() {
        return indexQueue.poll();
    }

    public boolean remove(EndPointIndexItem i) {
        return indexQueue.remove(i);
    }

    public EndPointIndexItem top() {
        return indexQueue.peek();
    }

    protected boolean continueBuildingIndex() {
        return continueBuldingIndex;
    }

    protected void stopBuildingIndex() {
        continueBuldingIndex = false;
    }

    protected int getMemoryPartition() {
        return partition;
    }

}