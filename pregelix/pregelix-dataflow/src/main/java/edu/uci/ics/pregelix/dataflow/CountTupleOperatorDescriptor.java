/*
 * Copyright 2009-2013 by The Regents of the University of California
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
package edu.uci.ics.pregelix.dataflow;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;

public class CountTupleOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {
    private static final long serialVersionUID = 1L;

    public CountTupleOperatorDescriptor(JobSpecification spec, RecordDescriptor rDesc) {
        super(spec, 1, 1);
        this.recordDescriptors[0] = rDesc;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
            final IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions)
            throws HyracksDataException {
        return new AbstractUnaryInputUnaryOutputOperatorNodePushable() {
            private final RecordDescriptor rd0 = recordDescProvider.getInputRecordDescriptor(getActivityId(), 0);
            private final FrameTupleAccessor fta = new FrameTupleAccessor(ctx.getFrameSize(), rd0);
            private int tupleCount = 0;

            @Override
            public void close() throws HyracksDataException {
                System.out.println(this.toString() + " tuple count " + tupleCount);
                writer.close();
            }

            @Override
            public void fail() throws HyracksDataException {
                writer.fail();
            }

            @Override
            public void nextFrame(ByteBuffer frame) throws HyracksDataException {
                fta.reset(frame);
                tupleCount += fta.getTupleCount();
                writer.nextFrame(frame);
            }

            @Override
            public void open() throws HyracksDataException {
                writer.open();
            }

        };
    }
}
