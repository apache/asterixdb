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

import java.nio.ByteBuffer;
import java.util.ArrayList;

import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.std.sort.util.DeletableFrameTupleAppender;
import org.apache.hyracks.dataflow.std.sort.util.IAppendDeletableFrameTupleAccessor;
import org.apache.hyracks.dataflow.std.structures.TuplePointer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Enable the delete record operation in the memory management. This is only used in the {@link org.apache.hyracks.dataflow.std.sort.HeapSortRunGenerator}
 */
public class VariableDeletableTupleMemoryManager implements IDeletableTupleBufferManager {

    private static final Logger LOG = LogManager.getLogger();

    private final int minFreeSpace;
    private final IFramePool pool;
    private final IFrameFreeSlotPolicy policy;
    private final IAppendDeletableFrameTupleAccessor accessor;
    private final ArrayList<ByteBuffer> frames;
    private final RecordDescriptor recordDescriptor;
    private int numTuples;
    private int statsReOrg;

    public VariableDeletableTupleMemoryManager(IFramePool framePool, RecordDescriptor recordDescriptor) {
        this.pool = framePool;
        int maxFrames = framePool.getMemoryBudgetBytes() / framePool.getMinFrameSize();
        this.policy = new FrameFreeSlotLastFit(maxFrames);
        this.accessor = new DeletableFrameTupleAppender(recordDescriptor);
        this.frames = new ArrayList<>();
        this.minFreeSpace = calculateMinFreeSpace(recordDescriptor);
        this.recordDescriptor = recordDescriptor;
        this.numTuples = 0;
        this.statsReOrg = 0;
    }

    @Override
    public void reset() throws HyracksDataException {
        pool.reset();
        policy.reset();
        frames.clear();
        numTuples = 0;
    }

    @Override
    public int getNumTuples() {
        return numTuples;
    }

    @Override
    public boolean insertTuple(IFrameTupleAccessor fta, int idx, TuplePointer tuplePointer)
            throws HyracksDataException {
        int requiredFreeSpace = calculatePhysicalSpace(fta, idx);
        int frameId = findAvailableFrame(requiredFreeSpace);
        if (frameId < 0) {
            if (canBeInsertedAfterCleanUpFragmentation(requiredFreeSpace)) {
                reOrganizeFrames();
                frameId = findAvailableFrame(requiredFreeSpace);
                statsReOrg++;
            } else {
                return false;
            }
        }
        assert frameId >= 0;
        accessor.reset(frames.get(frameId));
        assert accessor.getContiguousFreeSpace() >= requiredFreeSpace;
        int tid = accessor.append(fta, idx);
        assert tid >= 0;
        tuplePointer.reset(frameId, tid);
        if (accessor.getContiguousFreeSpace() > minFreeSpace) {
            policy.pushNewFrame(frameId, accessor.getContiguousFreeSpace());
        }
        numTuples++;
        return true;
    }

    private void reOrganizeFrames() {
        policy.reset();
        for (int i = 0; i < frames.size(); i++) {
            accessor.reset(frames.get(i));
            accessor.reOrganizeBuffer();
            policy.pushNewFrame(i, accessor.getContiguousFreeSpace());
        }
    }

    private boolean canBeInsertedAfterCleanUpFragmentation(int requiredFreeSpace) {
        for (int i = 0; i < frames.size(); i++) {
            accessor.reset(frames.get(i));
            if (accessor.getTotalFreeSpace() >= requiredFreeSpace) {
                return true;
            }
        }
        return false;
    }

    private int findAvailableFrame(int requiredFreeSpace) throws HyracksDataException {
        int frameId = policy.popBestFit(requiredFreeSpace);
        if (frameId >= 0) {
            return frameId;
        }

        int frameSize = calculateMinFrameSizeToPlaceTuple(requiredFreeSpace, pool.getMinFrameSize());
        ByteBuffer buffer = pool.allocateFrame(frameSize);
        if (buffer != null) {
            accessor.clear(buffer);
            frames.add(buffer);
            return frames.size() - 1;
        }
        return -1;
    }

    private static int calculateMinFrameSizeToPlaceTuple(int requiredFreeSpace, int minFrameSize) {
        return (1 + (requiredFreeSpace + 4 - 1) / minFrameSize) * minFrameSize;
    }

    private static int calculatePhysicalSpace(IFrameTupleAccessor fta, int idx) {
        // 4 bytes to store the offset
        return 4 + fta.getTupleLength(idx);
    }

    private static int calculateMinFreeSpace(RecordDescriptor recordDescriptor) {
        // + 4 for the tuple offset
        return recordDescriptor.getFieldCount() * 4 + 4;
    }

    @Override
    public void deleteTuple(TuplePointer tuplePointer) throws HyracksDataException {
        accessor.reset(frames.get(tuplePointer.getFrameIndex()));
        accessor.delete(tuplePointer.getTupleIndex());
        numTuples--;
    }

    @Override
    public void close() {
        pool.close();
        policy.close();
        frames.clear();
        numTuples = 0;
        if (LOG.isDebugEnabled()) {
            LOG.debug("VariableTupleMemoryManager has reorganized " + statsReOrg + " times");
        }
        statsReOrg = 0;
    }

    @Override
    public ITuplePointerAccessor createTuplePointerAccessor() {
        return new AbstractTuplePointerAccessor() {
            private final IAppendDeletableFrameTupleAccessor bufferAccessor =
                    new DeletableFrameTupleAppender(recordDescriptor);

            @Override
            IFrameTupleAccessor getInnerAccessor() {
                return bufferAccessor;
            }

            @Override
            void resetInnerAccessor(TuplePointer tuplePointer) {
                bufferAccessor.reset(frames.get(tuplePointer.getFrameIndex()));
            }
        };
    }

}
