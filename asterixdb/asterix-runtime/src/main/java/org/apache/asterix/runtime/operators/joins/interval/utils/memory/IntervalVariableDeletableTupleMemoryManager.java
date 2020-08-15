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
package org.apache.asterix.runtime.operators.joins.interval.utils.memory;

import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.dataflow.std.buffermanager.IFramePool;
import org.apache.hyracks.dataflow.std.buffermanager.VariableDeletableTupleMemoryManager;
import org.apache.hyracks.dataflow.std.sort.util.DeletableFrameTupleAppender;
import org.apache.hyracks.dataflow.std.sort.util.IAppendDeletableFrameTupleAccessor;
import org.apache.hyracks.dataflow.std.structures.TuplePointer;

public class IntervalVariableDeletableTupleMemoryManager extends VariableDeletableTupleMemoryManager {
    public IntervalVariableDeletableTupleMemoryManager(IFramePool framePool, RecordDescriptor recordDescriptor) {
        super(framePool, recordDescriptor);
    }

    public ITupleAccessor createTupleAccessor() {
        return new AbstractTupleAccessor() {
            private IAppendDeletableFrameTupleAccessor bufferAccessor =
                    new DeletableFrameTupleAppender(recordDescriptor);

            @Override
            protected IFrameTupleAccessor getInnerAccessor() {
                return bufferAccessor;
            }

            protected void resetInnerAccessor(TuplePointer tuplePointer) {
                bufferAccessor.reset(frames.get(tuplePointer.getFrameIndex()));
            }

            @Override
            protected void resetInnerAccessor(int frameIndex) {
                bufferAccessor.reset(frames.get(frameIndex));
            }

            @Override
            protected int getFrameCount() {
                return frames.size();
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
