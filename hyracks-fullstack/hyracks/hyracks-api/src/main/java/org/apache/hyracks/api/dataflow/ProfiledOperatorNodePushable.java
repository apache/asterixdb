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
package org.apache.hyracks.api.dataflow;

import java.util.HashMap;
import java.util.Map;

import org.apache.hyracks.api.com.job.profiling.counters.Counter;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.profiling.IOperatorStats;
import org.apache.hyracks.api.job.profiling.IStatsCollector;
import org.apache.hyracks.api.job.profiling.OperatorStats;
import org.apache.hyracks.api.job.profiling.counters.ICounter;
import org.apache.hyracks.api.rewriter.runtime.SuperActivityOperatorNodePushable;

public class ProfiledOperatorNodePushable implements IOperatorNodePushable, IStatsContainingNodePushable {

    private final IOperatorNodePushable op;
    private final Map<Integer, ITimedWriter> inputs;
    private final Map<Integer, ITimedWriter> outputs;
    private final IOperatorStats stats;
    private final ICounter totalTime;

    ProfiledOperatorNodePushable(IOperatorNodePushable op, IOperatorStats stats) {
        this.stats = stats;
        this.op = op;
        inputs = new HashMap<>();
        outputs = new HashMap<>();
        this.totalTime = new Counter("totalTime");
    }

    @Override
    public void initialize() throws HyracksDataException {
        ProfiledFrameWriter.timeMethod(op::initialize, totalTime);
    }

    @Override
    public void deinitialize() throws HyracksDataException {
        long ownTime = totalTime.get();
        for (ITimedWriter i : inputs.values()) {
            ownTime += i.getTotalTime();
        }
        for (ITimedWriter w : outputs.values()) {
            ownTime -= w.getTotalTime();
        }
        op.deinitialize();
        stats.getTimeCounter().set(ownTime);
    }

    @Override
    public int getInputArity() {
        return op.getInputArity();
    }

    @Override
    public void setOutputFrameWriter(int index, IFrameWriter writer, RecordDescriptor recordDesc)
            throws HyracksDataException {
        if (writer instanceof ITimedWriter) {
            ITimedWriter wrapper = (ITimedWriter) writer;
            if (op instanceof ISelfProfilingNodePushable) {
                wrapper.setUpstreamStats(((ISelfProfilingNodePushable) op).getStats());
            } else {
                wrapper.setUpstreamStats(stats);
            }
            outputs.put(index, wrapper);
        }
        op.setOutputFrameWriter(index, writer, recordDesc);
    }

    @Override
    public IFrameWriter getInputFrameWriter(int index) {
        if (inputs.get(index) == null) {
            ProfiledFrameWriter pfw = new ProfiledFrameWriter(op.getInputFrameWriter(index));
            inputs.put(index, pfw);
            return pfw;
        } else {
            return inputs.get(index);
        }
    }

    @Override
    public String getDisplayName() {
        return op.getDisplayName();
    }

    public IOperatorStats getStats() {
        return stats;
    }

    public static IOperatorNodePushable time(IOperatorNodePushable op, IHyracksTaskContext ctx, ActivityId acId)
            throws HyracksDataException {
        String name = acId.toString() + " - " + op.getDisplayName();
        IStatsCollector statsCollector = ctx.getStatsCollector();
        IOperatorStats stats = new OperatorStats(name, acId.getOperatorDescriptorId().toString());
        if (!(op instanceof ISelfProfilingNodePushable)) {
            statsCollector.add(stats);
        }
        if (op instanceof IIntrospectingOperator) {
            ((IIntrospectingOperator) op).setOperatorStats(stats);
        }
        if (op instanceof ISelfProfilingNodePushable) {
            ((ISelfProfilingNodePushable) op).addStats(stats);
        }
        if (!(op instanceof ProfiledOperatorNodePushable) && !(op instanceof SuperActivityOperatorNodePushable)) {
            return new ProfiledOperatorNodePushable(op, stats);
        }
        return op;
    }

    public static void onlyAddStats(IOperatorNodePushable op, IHyracksTaskContext ctx, ActivityId acId)
            throws HyracksDataException {
        String name = acId.toString() + " - " + op.getDisplayName();
        IStatsCollector statsCollector = ctx.getStatsCollector();
        IOperatorStats stats = new OperatorStats(name, acId.getOperatorDescriptorId().toString());
        if (op instanceof IIntrospectingOperator) {
            ((IIntrospectingOperator) op).setOperatorStats(stats);
            statsCollector.add(stats);
        }
    }
}
