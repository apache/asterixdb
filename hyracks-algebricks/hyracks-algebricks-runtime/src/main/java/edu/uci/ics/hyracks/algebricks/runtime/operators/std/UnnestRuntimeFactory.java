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
package edu.uci.ics.hyracks.algebricks.runtime.operators.std;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.base.IUnnestingFunction;
import edu.uci.ics.hyracks.algebricks.runtime.base.IUnnestingFunctionFactory;
import edu.uci.ics.hyracks.algebricks.runtime.context.RuntimeContext;
import edu.uci.ics.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputOneFramePushRuntime;
import edu.uci.ics.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputRuntimeFactory;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ArrayBackedValueStorage;

public class UnnestRuntimeFactory extends AbstractOneInputOneOutputRuntimeFactory {

    private static final long serialVersionUID = 1L;

    private final int outCol;
    private final IUnnestingFunctionFactory unnestingFactory;
    private int outColPos;
    private final boolean outColIsProjected;

    // Each time step() is called on the aggregate, a new value is written in
    // its output. One byte is written before that value and is neglected.
    // By convention, if the aggregate function writes nothing, it means it
    // produced the last value.

    public UnnestRuntimeFactory(int outCol, IUnnestingFunctionFactory unnestingFactory, int[] projectionList) {
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
    }

    @Override
    public String toString() {
        return "unnest " + outCol + " <- " + unnestingFactory;
    }

    @Override
    public AbstractOneInputOneOutputOneFramePushRuntime createOneOutputPushRuntime(final RuntimeContext context)
            throws AlgebricksException {

        return new AbstractOneInputOneOutputOneFramePushRuntime() {

            private ArrayBackedValueStorage evalOutput;
            private IUnnestingFunction agg;
            private ArrayTupleBuilder tupleBuilder;

            @Override
            public void open() throws HyracksDataException {
                initAccessAppendRef(context);
                evalOutput = new ArrayBackedValueStorage();
                try {
                    agg = unnestingFactory.createUnnestingFunction(evalOutput);
                } catch (AlgebricksException ae) {
                    throw new HyracksDataException(ae);
                }
                tupleBuilder = new ArrayTupleBuilder(projectionList.length);
                writer.open();
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                tAccess.reset(buffer);
                int nTuple = tAccess.getTupleCount();
                for (int t = 0; t < nTuple; t++) {
                    tRef.reset(tAccess, t);
                    try {
                        agg.init(tRef);
                        boolean goon = true;
                        do {
                            tupleBuilder.reset();
                            evalOutput.reset();
                            if (!agg.step()) {
                                goon = false;
                            } else {
                                if (!outColIsProjected) {
                                    appendProjectionToFrame(t, projectionList);
                                } else {
                                    for (int f = 0; f < outColPos; f++) {
                                        tupleBuilder.addField(tAccess, t, f);
                                    }
                                    tupleBuilder.addField(evalOutput.getBytes(), evalOutput.getStartIndex(),
                                            evalOutput.getLength());
                                    for (int f = outColPos + 1; f < projectionList.length; f++) {
                                        tupleBuilder.addField(tAccess, t, f);
                                    }
                                }
                                appendToFrameFromTupleBuilder(tupleBuilder);
                            }
                        } while (goon);
                    } catch (AlgebricksException ae) {
                        throw new HyracksDataException(ae);
                    }
                }
            }
        };
    }

}
