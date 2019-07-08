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
package org.apache.hyracks.algebricks.runtime.operators.std;

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IUnnestingEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IUnnestingEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.base.IUnnestingPositionWriter;
import org.apache.hyracks.algebricks.runtime.evaluators.EvaluatorContext;
import org.apache.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputOneFramePushRuntime;
import org.apache.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputRuntimeFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IMissingWriter;
import org.apache.hyracks.api.dataflow.value.IMissingWriterFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;

public class UnnestRuntimeFactory extends AbstractOneInputOneOutputRuntimeFactory {

    private static final long serialVersionUID = 1L;

    private final int outCol;
    private final IUnnestingEvaluatorFactory unnestingFactory;
    private final boolean unnestColIsProjected;
    private final IUnnestingPositionWriter positionWriter;
    private final boolean leftOuter;
    private final IMissingWriterFactory missingWriterFactory;
    private int outColPos;

    public UnnestRuntimeFactory(int outCol, IUnnestingEvaluatorFactory unnestingFactory, int[] projectionList,
            boolean leftOuter, IMissingWriterFactory missingWriterFactory) {
        this(outCol, unnestingFactory, projectionList, null, leftOuter, missingWriterFactory);
    }

    public UnnestRuntimeFactory(int outCol, IUnnestingEvaluatorFactory unnestingFactory, int[] projectionList,
            IUnnestingPositionWriter positionWriter, boolean leftOuter, IMissingWriterFactory missingWriterFactory) {
        super(projectionList);
        this.outCol = outCol;
        this.unnestingFactory = unnestingFactory;
        outColPos = -1;
        for (int f = 0; f < projectionList.length; f++) {
            if (projectionList[f] == outCol) {
                outColPos = f;
            }
        }
        unnestColIsProjected = outColPos >= 0;
        this.positionWriter = positionWriter;
        this.leftOuter = leftOuter;
        this.missingWriterFactory = missingWriterFactory;
    }

    @Override
    public String toString() {
        return "unnest " + outCol + " <- " + unnestingFactory;
    }

    @Override
    public AbstractOneInputOneOutputOneFramePushRuntime createOneOutputPushRuntime(final IHyracksTaskContext ctx)
            throws HyracksDataException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutput output = new DataOutputStream(bos);
        if (missingWriterFactory != null) {
            IMissingWriter missingWriter = missingWriterFactory.createMissingWriter();
            missingWriter.writeMissing(output);
        }
        byte[] missingBytes = bos.toByteArray();
        int missingBytesLen = bos.size();
        IEvaluatorContext evalCtx = new EvaluatorContext(ctx);
        return new AbstractOneInputOneOutputOneFramePushRuntime() {
            private IPointable p = VoidPointable.FACTORY.createPointable();
            private ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(projectionList.length);
            private IUnnestingEvaluator unnest = unnestingFactory.createUnnestingEvaluator(evalCtx);

            @Override
            public void open() throws HyracksDataException {
                super.open();
                if (tRef == null) {
                    initAccessAppendRef(ctx);
                }
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                tAccess.reset(buffer);
                int nTuple = tAccess.getTupleCount();
                for (int t = 0; t < nTuple; t++) {
                    tRef.reset(tAccess, t);
                    try {
                        unnest.init(tRef);
                        unnesting(t);
                    } catch (IOException ae) {
                        throw HyracksDataException.create(ae);
                    }
                }
            }

            private void unnesting(int t) throws IOException {
                // Assumes that when unnesting the tuple, each step() call for each element
                // in the tuple will increase the positionIndex, and the positionIndex will
                // be reset when a new tuple is to be processed.
                int positionIndex = 1;
                boolean emitted = false;
                do {
                    if (!unnest.step(p)) {
                        break;
                    }
                    writeOutput(t, positionIndex++, false);
                    emitted = true;
                } while (true);
                if (leftOuter && !emitted) {
                    writeOutput(t, -1, true);
                }
            }

            private void writeOutput(int t, int positionIndex, boolean missing)
                    throws HyracksDataException, IOException {
                if (!unnestColIsProjected && positionWriter == null) {
                    appendProjectionToFrame(t, projectionList);
                    appendToFrameFromTupleBuilder(tupleBuilder);
                    return;
                }

                tupleBuilder.reset();
                for (int f = 0; f < outColPos; f++) {
                    tupleBuilder.addField(tAccess, t, f);
                }
                if (unnestColIsProjected) {
                    if (missing) {
                        tupleBuilder.addField(missingBytes, 0, missingBytesLen);
                    } else {
                        tupleBuilder.addField(p.getByteArray(), p.getStartOffset(), p.getLength());
                    }
                }
                for (int f = unnestColIsProjected ? outColPos + 1 : outColPos; f < (positionWriter != null
                        ? projectionList.length - 1 : projectionList.length); f++) {
                    tupleBuilder.addField(tAccess, t, f);
                }
                if (positionWriter != null) {
                    // Write the positional variable
                    if (missing) {
                        tupleBuilder.addField(missingBytes, 0, missingBytesLen);
                    } else {
                        positionWriter.write(tupleBuilder.getDataOutput(), positionIndex);
                        tupleBuilder.addFieldEndOffset();
                    }
                }
                appendToFrameFromTupleBuilder(tupleBuilder);
            }

            @Override
            public void flush() throws HyracksDataException {
                appender.flush(writer);
            }
        };
    }
}
