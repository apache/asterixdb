/*
 * Copyright 2009-2010 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.coreops.util;

import java.nio.ByteBuffer;
import java.util.List;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.dataflow.ActivityNodeId;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobPlan;
import edu.uci.ics.hyracks.comm.io.FrameDeserializer;
import edu.uci.ics.hyracks.comm.io.SerializingDataWriter;
import edu.uci.ics.hyracks.context.HyracksContext;
import edu.uci.ics.hyracks.coreops.base.IOpenableDataWriterOperator;

public final class DeserializedOperatorNodePushable implements IOperatorNodePushable {
    private final HyracksContext ctx;

    private final IOpenableDataWriterOperator delegate;

    private final JobPlan plan;

    private final ActivityNodeId aid;

    private final FrameDeserializer deserializer;

    public DeserializedOperatorNodePushable(HyracksContext ctx, IOpenableDataWriterOperator delegate, JobPlan plan,
            ActivityNodeId aid) {
        this.ctx = ctx;
        this.delegate = delegate;
        this.plan = plan;
        this.aid = aid;
        List<Integer> inList = plan.getTaskInputMap().get(aid);
        deserializer = inList == null ? null : new FrameDeserializer(ctx, plan.getTaskInputRecordDescriptor(aid, 0));
    }

    @Override
    public void setFrameWriter(int index, IFrameWriter writer) {
        delegate.setDataWriter(index, new SerializingDataWriter(ctx, plan.getTaskOutputRecordDescriptor(aid, index),
                writer));
    }

    @Override
    public void close() throws HyracksDataException {
        delegate.close();
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        deserializer.reset(buffer);
        while (!deserializer.done()) {
            delegate.writeData(deserializer.deserializeRecord());
        }
    }

    @Override
    public void open() throws HyracksDataException {
        delegate.open();
    }
}