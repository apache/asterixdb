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
import java.util.Arrays;

import org.apache.hyracks.api.comm.FixedSizeFrame;
import org.apache.hyracks.api.comm.FrameHelper;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksFrameMgrContext;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.FixedSizeFrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.std.structures.TuplePointer;

/**
 * This buffer manager will divide the buffers into given number of partitions.
 * The cleared partition (spilled one in the caller side) can only get no more than one frame.
 */
public class VPartitionTupleBufferManager implements IPartitionedTupleBufferManager {
    public static final IPartitionedMemoryConstrain NO_CONSTRAIN = new IPartitionedMemoryConstrain() {
        @Override
        public int frameLimit(int partitionId) {
            return Integer.MAX_VALUE;
        }
    };

    private IDeallocatableFramePool framePool;
    private IFrameBufferManager[] partitionArray;
    private int[] numTuples;
    private final FixedSizeFrame appendFrame;
    private final FixedSizeFrameTupleAppender appender;
    private BufferInfo tempInfo;
    private IPartitionedMemoryConstrain constrain;

    // In case where a frame pool is shared by one or more buffer manager(s), it can be provided from the caller.
    public VPartitionTupleBufferManager(IPartitionedMemoryConstrain constrain, int partitions,
            IDeallocatableFramePool framePool) throws HyracksDataException {
        this.constrain = constrain;
        this.framePool = framePool;
        this.partitionArray = new IFrameBufferManager[partitions];
        this.numTuples = new int[partitions];
        this.appendFrame = new FixedSizeFrame();
        this.appender = new FixedSizeFrameTupleAppender();
        this.tempInfo = new BufferInfo(null, -1, -1);
    }

    public VPartitionTupleBufferManager(IHyracksFrameMgrContext ctx, IPartitionedMemoryConstrain constrain,
            int partitions, int frameLimitInBytes) throws HyracksDataException {
        this.constrain = constrain;
        this.framePool = new DeallocatableFramePool(ctx, frameLimitInBytes);
        this.partitionArray = new IFrameBufferManager[partitions];
        this.numTuples = new int[partitions];
        this.appendFrame = new FixedSizeFrame();
        this.appender = new FixedSizeFrameTupleAppender();
        this.tempInfo = new BufferInfo(null, -1, -1);
    }

    @Override
    public void setConstrain(IPartitionedMemoryConstrain constrain) {
        this.constrain = constrain;
    }

    @Override
    public void reset() throws HyracksDataException {
        for (IFrameBufferManager part : partitionArray) {
            if (part != null) {
                for (int i = 0; i < part.getNumFrames(); i++) {
                    framePool.deAllocateBuffer(part.getFrame(i, tempInfo).getBuffer());
                }
                part.reset();
            }
        }
        Arrays.fill(numTuples, 0);
        appendFrame.reset(null);
    }

    @Override
    public int getNumPartitions() {
        return partitionArray.length;
    }

    @Override
    public int getNumTuples(int partition) {
        return numTuples[partition];
    }

    @Override
    public int getPhysicalSize(int partitionId) {
        int size = 0;
        IFrameBufferManager partition = partitionArray[partitionId];
        if (partition != null) {
            for (int i = 0; i < partition.getNumFrames(); ++i) {
                size += partition.getFrame(i, tempInfo).getLength();
            }
        }
        return size;
    }

    @Override
    public void clearPartition(int partitionId) throws HyracksDataException {
        IFrameBufferManager partition = partitionArray[partitionId];
        if (partition != null) {
            for (int i = 0; i < partition.getNumFrames(); ++i) {
                framePool.deAllocateBuffer(partition.getFrame(i, tempInfo).getBuffer());
            }
            partition.reset();
        }
        numTuples[partitionId] = 0;
    }

    @Override
    public boolean insertTuple(int partition, byte[] byteArray, int[] fieldEndOffsets, int start, int size,
            TuplePointer pointer) throws HyracksDataException {
        int fieldCount = fieldEndOffsets == null ? 0 : fieldEndOffsets.length;
        int fid = getLastBufferOrCreateNewIfNotExist(partition, size, fieldCount);
        if (fid < 0) {
            return false;
        }
        partitionArray[partition].getFrame(fid, tempInfo);
        int tid = appendTupleToBuffer(tempInfo, fieldEndOffsets, byteArray, start, size);
        if (tid < 0) {
            fid = createNewBuffer(partition, size, fieldCount);
            if (fid < 0) {
                return false;
            }
            partitionArray[partition].getFrame(fid, tempInfo);
            tid = appendTupleToBuffer(tempInfo, fieldEndOffsets, byteArray, start, size);
        }
        pointer.reset(makeGroupFrameId(partition, fid), tid);
        numTuples[partition]++;
        return true;
    }

    @Override
    public boolean insertTuple(int partition, IFrameTupleAccessor tupleAccessor, int tupleId, TuplePointer pointer)
            throws HyracksDataException {
        return insertTuple(partition, tupleAccessor.getBuffer().array(), null,
                tupleAccessor.getTupleStartOffset(tupleId), tupleAccessor.getTupleLength(tupleId), pointer);
    }

    @Override
    public void cancelInsertTuple(int partition) throws HyracksDataException {
        int fid = getLastBuffer(partition);
        if (fid < 0) {
            throw new HyracksDataException("Couldn't get the last frame for the given partition.");
        }
        partitionArray[partition].getFrame(fid, tempInfo);
        deleteTupleFromBuffer(tempInfo);
        numTuples[partition]--;
    }

    @Override
    public int framesNeeded(int tupleSize, int fieldCount) {
        int minFrameSize = framePool.getMinFrameSize();
        return FrameHelper.calcAlignedFrameSizeToStore(fieldCount, tupleSize, minFrameSize) / minFrameSize;
    }

    public static int calculateActualSize(int[] fieldEndOffsets, int size) {
        if (fieldEndOffsets != null) {
            return FrameHelper.calcRequiredSpace(fieldEndOffsets.length, size);
        }
        return FrameHelper.calcRequiredSpace(0, size);
    }

    private int makeGroupFrameId(int partition, int fid) {
        return fid * getNumPartitions() + partition;
    }

    private int parsePartitionId(int externalFrameId) {
        return externalFrameId % getNumPartitions();
    }

    private int parseFrameIdInPartition(int externalFrameId) {
        return externalFrameId / getNumPartitions();
    }

    private int createNewBuffer(int partition, int tupleSize, int fieldCount) throws HyracksDataException {
        ByteBuffer newBuffer = requestNewBufferFromPool(tupleSize, partition, fieldCount);
        if (newBuffer == null) {
            return -1;
        }
        appendFrame.reset(newBuffer);
        appender.reset(appendFrame, true);
        return partitionArray[partition].insertFrame(newBuffer);
    }

    private ByteBuffer requestNewBufferFromPool(int recordSize, int partition, int fieldCount)
            throws HyracksDataException {
        int minFrameSize = framePool.getMinFrameSize();
        int frameSize = FrameHelper.calcAlignedFrameSizeToStore(fieldCount, recordSize, minFrameSize);
        if ((double) frameSize / (double) minFrameSize + getPhysicalSize(partition) / (double) minFrameSize > constrain
                .frameLimit(partition)) {
            return null;
        }
        return framePool.allocateFrame(frameSize);
    }

    private int appendTupleToBuffer(BufferInfo bufferInfo, int[] fieldEndOffsets, byte[] byteArray, int start, int size)
            throws HyracksDataException {
        assert bufferInfo.getStartOffset() == 0 : "Haven't supported yet in FrameTupleAppender";
        if (bufferInfo.getBuffer() != appendFrame.getBuffer()) {
            appendFrame.reset(bufferInfo.getBuffer());
            appender.reset(appendFrame, false);
        }
        if (fieldEndOffsets == null) {
            if (appender.append(byteArray, start, size)) {
                return appender.getTupleCount() - 1;
            }
        } else {
            if (appender.append(fieldEndOffsets, byteArray, start, size)) {
                return appender.getTupleCount() - 1;
            }
        }

        return -1;
    }

    private void deleteTupleFromBuffer(BufferInfo bufferInfo) throws HyracksDataException {
        if (bufferInfo.getBuffer() != appendFrame.getBuffer()) {
            appendFrame.reset(bufferInfo.getBuffer());
            appender.reset(appendFrame, false);
        }
        if (!appender.cancelAppend()) {
            throw new HyracksDataException("Undoing the last insertion in the given frame couldn't be done.");
        }
    }

    private int getLastBufferOrCreateNewIfNotExist(int partition, int tupleSize, int fieldCount)
            throws HyracksDataException {
        if (partitionArray[partition] == null || partitionArray[partition].getNumFrames() == 0) {
            partitionArray[partition] = new FrameBufferManager();
            return createNewBuffer(partition, tupleSize, fieldCount);
        }
        return getLastBuffer(partition);
    }

    private int getLastBuffer(int partition) throws HyracksDataException {
        return partitionArray[partition].getNumFrames() - 1;
    }

    @Override
    public void close() {
        for (IFrameBufferManager part : partitionArray) {
            if (part != null) {
                part.close();
            }
        }
        framePool.close();
        Arrays.fill(partitionArray, null);
    }

    @Override
    public ITuplePointerAccessor getTuplePointerAccessor(final RecordDescriptor recordDescriptor) {
        return new AbstractTuplePointerAccessor() {
            FrameTupleAccessor innerAccessor = new FrameTupleAccessor(recordDescriptor);

            @Override
            IFrameTupleAccessor getInnerAccessor() {
                return innerAccessor;
            }

            @Override
            void resetInnerAccessor(TuplePointer tuplePointer) {
                partitionArray[parsePartitionId(tuplePointer.getFrameIndex())]
                        .getFrame(parseFrameIdInPartition(tuplePointer.getFrameIndex()), tempInfo);
                innerAccessor.reset(tempInfo.getBuffer(), tempInfo.getStartOffset(), tempInfo.getLength());
            }
        };
    }

    @Override
    public int flushPartition(int pid, IFrameWriter writer) throws HyracksDataException {
        IFrameBufferManager partition = partitionArray[pid];
        int written = 0;
        if (partition != null && getNumTuples(pid) > 0) {
            for (int i = 0; i < partition.getNumFrames(); ++i) {
                partition.getFrame(i, tempInfo);
                tempInfo.getBuffer().position(tempInfo.getStartOffset());
                tempInfo.getBuffer().limit(tempInfo.getStartOffset() + tempInfo.getLength());
                int sz = tempInfo.getLength();
                writer.nextFrame(tempInfo.getBuffer());
                written += sz;
            }
        }
        return written;
    }

    @Override
    public IPartitionedMemoryConstrain getConstrain() {
        return constrain;
    }

}
