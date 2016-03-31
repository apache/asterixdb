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

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.base.IUnnestingEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IUnnestingEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.base.IUnnestingPositionWriter;
import org.apache.hyracks.algebricks.runtime.evaluators.ConstantEvalFactory;
import org.apache.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputOneFramePushRuntime;
import org.apache.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputRuntimeFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;

public class UnnestRuntimeFactory extends AbstractOneInputOneOutputRuntimeFactory {

    private static final long serialVersionUID = 1L;

    private final int outCol;
    private final IUnnestingEvaluatorFactory unnestingFactory;
    private int outColPos;
    private final boolean outColIsProjected;

    private final IUnnestingPositionWriter positionWriter;
    private IScalarEvaluatorFactory posOffsetEvalFactory;

    // Each time step() is called on the aggregate, a new value is written in
    // its output. One byte is written before that value and is neglected.
    // By convention, if the aggregate function writes nothing, it means it
    // produced the last value.

    public UnnestRuntimeFactory(int outCol, IUnnestingEvaluatorFactory unnestingFactory, int[] projectionList) {
        this(outCol, unnestingFactory, projectionList, null, null);
    }

    public UnnestRuntimeFactory(int outCol, IUnnestingEvaluatorFactory unnestingFactory, int[] projectionList,
            IUnnestingPositionWriter positionWriter, IScalarEvaluatorFactory posOffsetEvalFactory) {
        super(projectionList);
        this.outCol = outCol;
        this.unnestingFactory = unnestingFactory;
        outColPos = -1;
        for (int f = 0; f < projectionList.length; f++) {
            if (projectionList[f] == outCol) {
                outColPos = f;
            }
        }
        outColIsProjected = outColPos >= 0;
        this.positionWriter = positionWriter;
        this.posOffsetEvalFactory = posOffsetEvalFactory;
        if (this.posOffsetEvalFactory == null) {
            this.posOffsetEvalFactory = new ConstantEvalFactory(new byte[5]);
        }
    }

    @Override
    public String toString() {
        return "unnest " + outCol + " <- " + unnestingFactory;
    }

    @Override
    public AbstractOneInputOneOutputOneFramePushRuntime createOneOutputPushRuntime(final IHyracksTaskContext ctx)
            throws AlgebricksException {

        return new AbstractOneInputOneOutputOneFramePushRuntime() {
            private IPointable p = VoidPointable.FACTORY.createPointable();
            private IUnnestingEvaluator unnest;
            private ArrayTupleBuilder tupleBuilder;

            private IScalarEvaluator offsetEval = posOffsetEvalFactory.createScalarEvaluator(ctx);

            @Override
            public void open() throws HyracksDataException {
                writer.open();
                initAccessAppendRef(ctx);
                try {
                    unnest = unnestingFactory.createUnnestingEvaluator(ctx);
                } catch (AlgebricksException ae) {
                    throw new HyracksDataException(ae);
                }
                tupleBuilder = new ArrayTupleBuilder(projectionList.length);
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                tAccess.reset(buffer);
                int nTuple = tAccess.getTupleCount();
                for (int t = 0; t < nTuple; t++) {
                    tRef.reset(tAccess, t);
                    try {
                        offsetEval.evaluate(tRef, p);
                    } catch (AlgebricksException e) {
                        throw new HyracksDataException(e);
                    }
                    int offset = IntegerPointable.getInteger(p.getByteArray(), p.getStartOffset());
                    try {
                        unnest.init(tRef);
                        // assume that when unnesting the tuple, each step() call for each element
                        // in the tuple will increase the positionIndex, and the positionIndex will
                        // be reset when a new tuple is to be processed.
                        int positionIndex = 1;
                        boolean goon = true;
                        do {
                            tupleBuilder.reset();
                            if (!unnest.step(p)) {
                                goon = false;
                            } else {

                                if (!outColIsProjected && positionWriter == null) {
                                    appendProjectionToFrame(t, projectionList);
                                } else {
                                    for (int f = 0; f < outColPos; f++) {
                                        tupleBuilder.addField(tAccess, t, f);
                                    }
                                    if (outColIsProjected) {
                                        tupleBuilder.addField(p.getByteArray(), p.getStartOffset(), p.getLength());
                                    } else {
                                        tupleBuilder.addField(tAccess, t, outColPos);
                                    }
                                    for (int f = outColPos + 1; f < (positionWriter != null ? projectionList.length - 1
                                            : projectionList.length); f++) {
                                        tupleBuilder.addField(tAccess, t, f);
                                    }
                                }
                                if (positionWriter != null) {
                                    // Write the positional variable
                                    positionWriter.write(tupleBuilder.getDataOutput(), offset + positionIndex++);
                                    tupleBuilder.addFieldEndOffset();
                                }
                                appendToFrameFromTupleBuilder(tupleBuilder);
                            }
                        } while (goon);
                    } catch (AlgebricksException | IOException ae) {
                        throw new HyracksDataException(ae);
                    }
                }
            }

            @Override
            public void flush() throws HyracksDataException {
                appender.flush(writer);
            }
        };
    }

}
