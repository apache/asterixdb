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

package org.apache.hyracks.dataflow.std.parallel.histogram;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import org.apache.hyracks.dataflow.std.parallel.base.ParallelRangeMapTaskState;

/**
 * @author michael
 */
public class ForwardOperatorNodePushable extends AbstractUnaryInputUnaryOutputOperatorNodePushable {

    private final IHyracksTaskContext ctx;
    private final RecordDescriptor sampleDesc;
    private ParallelRangeMapTaskState sampleState;
    private int partition;

    public ForwardOperatorNodePushable(IHyracksTaskContext ctx, RecordDescriptor sampleDesc, int partition) {
        this.ctx = ctx;
        this.sampleDesc = sampleDesc;
        this.partition = partition;
    }

    @Override
    public void open() throws HyracksDataException {
        ctx.setGlobalState(partition, new ParallelRangeMapTaskState(sampleDesc));
        sampleState = (ParallelRangeMapTaskState) ctx.getGlobalState(partition);
        sampleState.open(ctx);
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        sampleState.appendFrame(buffer);
    }

    @Override
    public void fail() throws HyracksDataException {
    }

    @Override
    public void close() throws HyracksDataException {
        sampleState.close();
    }

}
