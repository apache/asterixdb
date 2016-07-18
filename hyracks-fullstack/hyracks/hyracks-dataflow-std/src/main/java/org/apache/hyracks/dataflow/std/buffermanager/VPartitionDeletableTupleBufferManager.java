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

import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.context.IHyracksFrameMgrContext;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.std.sort.util.DeletableFrameTupleAppender;
import org.apache.hyracks.dataflow.std.sort.util.IAppendDeletableFrameTupleAccessor;
import org.apache.hyracks.dataflow.std.structures.TuplePointer;

/**
 * This buffer manager will divide the buffers into given number of partitions.
 * The cleared partition (spilled one in the caller side) can only get no more than one frame.
 */
public class VPartitionDeletableTupleBufferManager extends VPartitionTupleBufferManager
        implements IPartitionedDeletableTupleBufferManager {

    private static int[] minFreeSpace;
    private final IAppendDeletableFrameTupleAccessor[] accessor;
    private final IFrameFreeSlotPolicy[] policy;

    public VPartitionDeletableTupleBufferManager(IHyracksFrameMgrContext ctx, IPartitionedMemoryConstrain constrain,
            int partitions, int frameLimitInBytes, RecordDescriptor[] recordDescriptors) throws HyracksDataException {
        super(ctx, constrain, partitions, frameLimitInBytes);
        int maxFrames = framePool.getMemoryBudgetBytes() / framePool.getMinFrameSize();
        this.policy = new FrameFreeSlotLastFit[partitions];
        this.accessor = new DeletableFrameTupleAppender[partitions];
        this.minFreeSpace = new int[partitions];
        int i = 0;
        for (RecordDescriptor rd : recordDescriptors) {
            this.accessor[i] = new DeletableFrameTupleAppender(rd);
            this.minFreeSpace[i] = calculateMinFreeSpace(rd);
            this.policy[i] = new FrameFreeSlotLastFit(maxFrames);
            ++i;
        }
    }

    @Override
    public boolean insertTuple(int partition, IFrameTupleAccessor fta, int idx, TuplePointer tuplePointer)
            throws HyracksDataException {
        int requiredFreeSpace = calculatePhysicalSpace(fta, idx);
        int frameId = findAvailableFrame(partition, requiredFreeSpace);
        if (frameId < 0) {
            if (canBeInsertedAfterCleanUpFragmentation(partition, requiredFreeSpace)) {
                reOrganizeFrames(partition);
                frameId = findAvailableFrame(partition, requiredFreeSpace);
            } else {
                return false;
            }
        }
        assert frameId >= 0;
        partitionArray[partition].getFrame(frameId, tempInfo);
        accessor[partition].reset(tempInfo.getBuffer());
        assert accessor[partition].getContiguousFreeSpace() >= requiredFreeSpace;
        int tid = accessor[partition].append(fta, idx);
        assert tid >= 0;
        if (accessor[partition].getContiguousFreeSpace() > minFreeSpace[partition]) {
            policy[partition].pushNewFrame(frameId, accessor[partition].getContiguousFreeSpace());
        }
        tuplePointer.reset(makeGroupFrameId(partition, frameId), tid);
        numTuples[partition]++;
        return true;
    }

    @Override
    public void clearPartition(int partitionId) throws HyracksDataException {
        IFrameBufferManager partition = partitionArray[partitionId];
        if (partition != null) {
            for (int i = 0; i < partition.getNumFrames(); ++i) {
                accessor[partitionId].clear(partition.getFrame(i, tempInfo).getBuffer());
            }
        }
        policy[partitionId].reset();
        super.clearPartition(partitionId);
    }

    private void reOrganizeFrames(int partition) {
        policy[partition].reset();
        for (int i = 0; i < partitionArray[partition].getNumFrames(); i++) {
            partitionArray[partition].getFrame(i, tempInfo);
            accessor[partition].reset(tempInfo.getBuffer());
            accessor[partition].reOrganizeBuffer();
            policy[partition].pushNewFrame(i, accessor[partition].getContiguousFreeSpace());
        }
    }

    private boolean canBeInsertedAfterCleanUpFragmentation(int partition, int requiredFreeSpace) {
        for (int i = 0; i < partitionArray[partition].getNumFrames(); i++) {
            partitionArray[partition].getFrame(i, tempInfo);
            accessor[partition].reset(tempInfo.getBuffer());
            if (accessor[partition].getTotalFreeSpace() >= requiredFreeSpace) {
                return true;
            }
        }
        return false;
    }

    private int findAvailableFrame(int partition, int requiredFreeSpace) throws HyracksDataException {
        int frameId = policy[partition].popBestFit(requiredFreeSpace);
        if (frameId >= 0) {
            return frameId;
        }
        if (partitionArray[partition] == null || partitionArray[partition].getNumFrames() == 0) {
            partitionArray[partition] = new PartitionFrameBufferManager();
        }
        return createNewBuffer(partition, requiredFreeSpace);
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
    public void deleteTuple(int partition, TuplePointer tuplePointer) throws HyracksDataException {
        partitionArray[parsePartitionId(tuplePointer.getFrameIndex())]
                .getFrame(parseFrameIdInPartition(tuplePointer.getFrameIndex()), tempInfo);
        accessor[partition].reset(tempInfo.getBuffer());
        accessor[partition].delete(tuplePointer.getTupleIndex());
        numTuples[partition]--;
    }

    @Override
    public ITuplePointerAccessor getTuplePointerAccessor(final RecordDescriptor recordDescriptor) {
        return new AbstractTuplePointerAccessor() {
            private IAppendDeletableFrameTupleAccessor innerAccessor = new DeletableFrameTupleAppender(
                    recordDescriptor);

            @Override
            IFrameTupleAccessor getInnerAccessor() {
                return innerAccessor;
            }

            @Override
            void resetInnerAccessor(TuplePointer tuplePointer) {
                partitionArray[parsePartitionId(tuplePointer.getFrameIndex())]
                        .getFrame(parseFrameIdInPartition(tuplePointer.getFrameIndex()), tempInfo);
                innerAccessor.reset(tempInfo.getBuffer());
            }
        };
    }

    @Override
    public ITupleAccessor getTupleAccessor(final RecordDescriptor recordDescriptor) {
        return new AbstractTupleAccessor() {
            private DeletableFrameTupleAppender innerAccessor = new DeletableFrameTupleAppender(recordDescriptor);

            @Override
            IFrameTupleAccessor getInnerAccessor() {
                return innerAccessor;
            }

            @Override
            void resetInnerAccessor(int frameIndex) {
                partitionArray[parsePartitionId(frameIndex)].getFrame(parseFrameIdInPartition(frameIndex), tempInfo);
                innerAccessor.reset(tempInfo.getBuffer());
            }

            @Override
            int getFrameCount() {
                return partitionArray.length;
            }

            @Override
            public boolean hasNext() {
                return hasNext(frameId, tupleId);
            }

            @Override
            public void next() {
                tupleId = nextTuple(frameId, tupleId);
                if (tupleId > INITIALIZED) {
                    return;
                }

                if (frameId + 1 < getFrameCount()) {
                    ++frameId;
                    resetInnerAccessor(frameId);
                    tupleId = INITIALIZED;
                    next();
                }
            }

            public boolean hasNext(int fId, int tId) {
                int id = nextTuple(fId, tId);
                if (id > INITIALIZED) {
                    return true;
                }
                if (fId + 1 < getFrameCount()) {
                    return hasNext(fId + 1, INITIALIZED);
                }
                return false;
            }

            public int nextTuple(int fId, int tId) {
                if (fId != frameId) {
                    resetInnerAccessor(fId);
                }
                int id = nextTupleInFrame(tId);
                if (fId != frameId) {
                    resetInnerAccessor(frameId);
                }
                return id;
            }

            public int nextTupleInFrame(int tId) {
                int id = tId;
                while (id + 1 < getTupleCount()) {
                    ++id;
                    if (getTupleEndOffset(id) > 0) {
                        return id;
                    }
                }
                return UNSET;
            }

        };
    }

}
