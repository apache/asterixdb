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
package edu.uci.ics.hyracks.algebricks.runtime.operators.std;

import java.io.IOException;
import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.runtime.base.IUnnestingEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.IUnnestingEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.runtime.evaluators.ConstantEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputOneFramePushRuntime;
import edu.uci.ics.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputRuntimeFactory;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.api.IPointable;
import edu.uci.ics.hyracks.data.std.primitive.VoidPointable;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;

public class UnnestRuntimeFactory extends AbstractOneInputOneOutputRuntimeFactory {

    private static final long serialVersionUID = 1L;

    private final int outCol;
    private final IUnnestingEvaluatorFactory unnestingFactory;
    private int outColPos;
    private final boolean outColIsProjected;

    private final boolean hasPositionalVariable;
    private IScalarEvaluatorFactory posOffsetEvalFactory;

    // Each time step() is called on the aggregate, a new value is written in
    // its output. One byte is written before that value and is neglected.
    // By convention, if the aggregate function writes nothing, it means it
    // produced the last value.

    public UnnestRuntimeFactory(int outCol, IUnnestingEvaluatorFactory unnestingFactory, int[] projectionList) {
        this(outCol, unnestingFactory, projectionList, false, null);
    }

    public UnnestRuntimeFactory(int outCol, IUnnestingEvaluatorFactory unnestingFactory, int[] projectionList,
            boolean hashPositionalVariable, IScalarEvaluatorFactory posOffsetEvalFactory) {
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
        this.hasPositionalVariable = hashPositionalVariable;
        this.posOffsetEvalFactory = posOffsetEvalFactory;
        if (this.posOffsetEvalFactory == null) {
            this.posOffsetEvalFactory = new ConstantEvaluatorFactory(new byte[5]);
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
            private IUnnestingEvaluator agg;
            private ArrayTupleBuilder tupleBuilder;

            private int tupleCount;
            private IScalarEvaluator offsetEval = posOffsetEvalFactory.createScalarEvaluator(ctx);

            @Override
            public void open() throws HyracksDataException {
                initAccessAppendRef(ctx);
                try {
                    agg = unnestingFactory.createUnnestingEvaluator(ctx);
                } catch (AlgebricksException ae) {
                    throw new HyracksDataException(ae);
                }
                tupleBuilder = new ArrayTupleBuilder(projectionList.length);
                tupleCount = 1;
                writer.open();
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

                    @SuppressWarnings("static-access")
                    int offset = IntegerSerializerDeserializer.INSTANCE.getInt(p.getByteArray(), p.getStartOffset());

                    try {
                        agg.init(tRef);
                        boolean goon = true;
                        do {
                            tupleBuilder.reset();
                            if (!agg.step(p)) {
                                goon = false;
                            } else {

                                if (!outColIsProjected && !hasPositionalVariable) {
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
                                    for (int f = outColPos + 1; f < (hasPositionalVariable ? projectionList.length - 1
                                            : projectionList.length); f++) {
                                        tupleBuilder.addField(tAccess, t, f);
                                    }
                                }
                                if (hasPositionalVariable) {
                                    // Write the positional variable as an INT32
                                    tupleBuilder.getDataOutput().writeByte(3);
                                    tupleBuilder.getDataOutput().writeInt(offset + tupleCount++);
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
        };
    }

}
