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
package edu.uci.ics.pregelix.dataflow.std;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameDeserializer;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputOperatorNodePushable;
import edu.uci.ics.pregelix.dataflow.std.base.IFunction;
import edu.uci.ics.pregelix.dataflow.std.base.IFunctionFactory;
import edu.uci.ics.pregelix.dataflow.std.base.IRecordDescriptorFactory;
import edu.uci.ics.pregelix.dataflow.std.base.IRuntimeHookFactory;

public class FunctionCallOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {
    private static final long serialVersionUID = 1L;
    private final IFunctionFactory functionFactory;
    private final IRuntimeHookFactory preHookFactory;
    private final IRuntimeHookFactory postHookFactory;
    private final IRecordDescriptorFactory inputRdFactory;

    public FunctionCallOperatorDescriptor(JobSpecification spec, IRecordDescriptorFactory inputRdFactory,
            int outputArity, IFunctionFactory functionFactory, IRuntimeHookFactory preHookFactory,
            IRuntimeHookFactory postHookFactory, RecordDescriptor... rDescs) {
        super(spec, 1, outputArity);
        this.functionFactory = functionFactory;
        this.preHookFactory = preHookFactory;
        this.postHookFactory = postHookFactory;
        this.inputRdFactory = inputRdFactory;

        for (int i = 0; i < rDescs.length; i++) {
            this.recordDescriptors[i] = rDescs[i];
        }
    }

    @Override
    public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
            final IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions)
            throws HyracksDataException {
        return new AbstractUnaryInputOperatorNodePushable() {
            private RecordDescriptor rd0;
            private FrameDeserializer frameDeserializer;
            private final IFrameWriter[] writers = new IFrameWriter[outputArity];
            private final IFunction function = functionFactory.createFunction();
            private ClassLoader ctxCL = Thread.currentThread().getContextClassLoader();

            @Override
            public void open() throws HyracksDataException {
                rd0 = inputRdFactory == null ? recordDescProvider.getInputRecordDescriptor(getActivityId(), 0)
                        : inputRdFactory.createRecordDescriptor(ctx);
                frameDeserializer = new FrameDeserializer(ctx.getFrameSize(), rd0);
                ctxCL = Thread.currentThread().getContextClassLoader();
                Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
                for (IFrameWriter writer : writers) {
                    writer.open();
                }
                if (preHookFactory != null)
                    preHookFactory.createRuntimeHook().configure(ctx);
                function.open(ctx, rd0, writers);
            }

            @Override
            public void nextFrame(ByteBuffer frame) throws HyracksDataException {
                frameDeserializer.reset(frame);
                while (!frameDeserializer.done()) {
                    Object[] tuple = frameDeserializer.deserializeRecord();
                    function.process(tuple);
                }
            }

            @Override
            public void close() throws HyracksDataException {
                if (postHookFactory != null)
                    postHookFactory.createRuntimeHook().configure(ctx);
                function.close();
                for (IFrameWriter writer : writers) {
                    writer.close();
                }
                Thread.currentThread().setContextClassLoader(ctxCL);
            }

            @Override
            public void fail() throws HyracksDataException {
                for (IFrameWriter writer : writers) {
                    writer.fail();
                }
            }

            @Override
            public void setOutputFrameWriter(int index, IFrameWriter writer, RecordDescriptor recordDesc) {
                writers[index] = writer;
            }
        };
    }
}
