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
package org.apache.hyracks.dataflow.std.join;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.std.buffermanager.ITupleAccessor;
import org.apache.hyracks.dataflow.std.buffermanager.TupleAccessor;
import org.apache.hyracks.dataflow.std.join.MergeBranchStatus.Stage;

public abstract class AbstractMergeJoiner implements IMergeJoiner {

    public enum TupleStatus {
        UNKNOWN,
        LOADED,
        EMPTY;

        public boolean isLoaded() {
            return this.equals(LOADED);
        }

        public boolean isEmpty() {
            return this.equals(EMPTY);
        }

        public boolean isKnown() {
            return !this.equals(UNKNOWN);
        }
    }

    protected static final int JOIN_PARTITIONS = 2;
    protected static final int LEFT_PARTITION = 0;
    protected static final int RIGHT_PARTITION = 1;

    protected final ByteBuffer[] inputBuffer;
    protected final FrameTupleAppender resultAppender;
    protected final ITupleAccessor[] inputAccessor;
    protected final MergeStatus status;

    private final int partition;
    private final MergeJoinLocks locks;
    protected long[] frameCounts = { 0, 0 };

    public AbstractMergeJoiner(IHyracksTaskContext ctx, int partition, MergeStatus status, MergeJoinLocks locks,
            RecordDescriptor leftRd, RecordDescriptor rightRd) throws HyracksDataException {
        this.partition = partition;
        this.status = status;
        this.locks = locks;

        inputAccessor = new TupleAccessor[JOIN_PARTITIONS];
        inputAccessor[LEFT_PARTITION] = new TupleAccessor(leftRd);
        inputAccessor[RIGHT_PARTITION] = new TupleAccessor(rightRd);

        inputBuffer = new ByteBuffer[JOIN_PARTITIONS];
        inputBuffer[LEFT_PARTITION] = ctx.allocateFrame();
        inputBuffer[RIGHT_PARTITION] = ctx.allocateFrame();

        // Result
        resultAppender = new FrameTupleAppender(new VSizeFrame(ctx));
    }

    public void setLeftFrame(ByteBuffer buffer) {
        setFrame(LEFT_PARTITION, buffer);
    }

    public void setRightFrame(ByteBuffer buffer) {
        setFrame(RIGHT_PARTITION, buffer);
    }

    protected TupleStatus loadMemoryTuple(int branch) {
        TupleStatus loaded;
        if (inputAccessor[branch] != null && inputAccessor[branch].exists()) {
            // Still processing frame.
            int test = inputAccessor[branch].getTupleCount();
            loaded = TupleStatus.LOADED;
        } else if (status.branch[branch].hasMore()) {
            loaded = TupleStatus.UNKNOWN;
        } else {
            // No more frames or tuples to process.
            loaded = TupleStatus.EMPTY;
        }
        return loaded;
    }

    protected TupleStatus pauseAndLoadRightTuple() {
        status.continueRightLoad = true;
        locks.getRight(partition).signal();
        try {
            while (status.continueRightLoad
                    && status.branch[RIGHT_PARTITION].getStatus().isEqualOrBefore(Stage.DATA_PROCESSING)) {
                locks.getLeft(partition).await();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        if (inputAccessor[RIGHT_PARTITION] != null && !inputAccessor[RIGHT_PARTITION].exists()
                && status.branch[RIGHT_PARTITION].getStatus() == Stage.CLOSED) {
            status.branch[RIGHT_PARTITION].noMore();
            return TupleStatus.EMPTY;
        }
        return TupleStatus.LOADED;
    }

    @Override
    public void setFrame(int branch, ByteBuffer buffer) {
        inputBuffer[branch].clear();
        if (inputBuffer[branch].capacity() < buffer.capacity()) {
            inputBuffer[branch].limit(buffer.capacity());
        }
        inputBuffer[branch].put(buffer.array(), 0, buffer.capacity());
        inputAccessor[branch].reset(inputBuffer[branch]);
        inputAccessor[branch].next();
        frameCounts[branch]++;
    }
}
