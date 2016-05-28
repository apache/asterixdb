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
    }

    protected static final int JOIN_PARTITIONS = 2;
    protected static final int LEFT_PARTITION = 0;
    protected static final int RIGHT_PARTITION = 1;

    protected final ITupleAccessor leftInputAccessor;
    protected final ITupleAccessor rightInputAccessor;

    private MergeJoinLocks locks;
    private MergeStatus status;

    protected ByteBuffer leftBuffer;
    protected ByteBuffer rightBuffer;

    private final int partition;

    protected FrameTupleAppender resultAppender;

    public AbstractMergeJoiner(IHyracksTaskContext ctx, int partition, MergeStatus status, MergeJoinLocks locks,
            RecordDescriptor leftRd, RecordDescriptor rightRd) throws HyracksDataException {
        this.partition = partition;
        this.status = status;
        this.locks = locks;

        leftInputAccessor = new TupleAccessor(leftRd);
        leftBuffer = ctx.allocateFrame();

        rightInputAccessor = new TupleAccessor(rightRd);
        rightBuffer = ctx.allocateFrame();

        // Result
        resultAppender = new FrameTupleAppender(new VSizeFrame(ctx));
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
        if (rightInputAccessor != null && !rightInputAccessor.exists()
                && status.branch[RIGHT_PARTITION].getStatus() == Stage.CLOSED) {
            status.branch[RIGHT_PARTITION].noMore();
            return TupleStatus.EMPTY;
        }
        return TupleStatus.LOADED;
    }

    @Override
    public void setLeftFrame(ByteBuffer buffer) {
        leftBuffer.clear();
        if (leftBuffer.capacity() < buffer.capacity()) {
            leftBuffer.limit(buffer.capacity());
        }
        leftBuffer.put(buffer.array(), 0, buffer.capacity());
        leftInputAccessor.reset(leftBuffer);
        leftInputAccessor.next();
    }

    @Override
    public void setRightFrame(ByteBuffer buffer) {
        rightBuffer.clear();
        if (rightBuffer.capacity() < buffer.capacity()) {
            rightBuffer.limit(buffer.capacity());
        }
        rightBuffer.put(buffer.array(), 0, buffer.capacity());
        rightInputAccessor.reset(rightBuffer);
        rightInputAccessor.next();
        status.continueRightLoad = false;
    }

}
