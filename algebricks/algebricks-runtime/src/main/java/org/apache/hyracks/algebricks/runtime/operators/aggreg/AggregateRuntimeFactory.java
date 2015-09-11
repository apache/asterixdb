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
package org.apache.hyracks.algebricks.runtime.operators.aggreg;

import java.nio.ByteBuffer;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.runtime.base.IAggregateEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IAggregateEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputOneFramePushRuntime;
import org.apache.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputRuntimeFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;

public class AggregateRuntimeFactory extends AbstractOneInputOneOutputRuntimeFactory {

    private static final long serialVersionUID = 1L;

    // private int[] outColumns;
    private IAggregateEvaluatorFactory[] aggregFactories;

    public AggregateRuntimeFactory(IAggregateEvaluatorFactory[] aggregFactories) {
        super(null);
        // this.outColumns = outColumns;
        this.aggregFactories = aggregFactories;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("assign [");
        for (int i = 0; i < aggregFactories.length; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(aggregFactories[i]);
        }
        sb.append("]");
        return sb.toString();
    }

    @Override
    public AbstractOneInputOneOutputOneFramePushRuntime createOneOutputPushRuntime(final IHyracksTaskContext ctx)
            throws AlgebricksException {
        return new AbstractOneInputOneOutputOneFramePushRuntime() {

            private IAggregateEvaluator[] aggregs = new IAggregateEvaluator[aggregFactories.length];
            private IPointable result = VoidPointable.FACTORY.createPointable();
            private ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(aggregs.length);

            private boolean first = true;

            @Override
            public void open() throws HyracksDataException {
                try {
                    if (first) {
                        first = false;
                        initAccessAppendRef(ctx);
                        for (int i = 0; i < aggregFactories.length; i++) {
                            aggregs[i] = aggregFactories[i].createAggregateEvaluator(ctx);
                        }
                    }
                    for (int i = 0; i < aggregFactories.length; i++) {
                        aggregs[i].init();
                    }
                } catch (AlgebricksException e) {
                    throw new HyracksDataException(e);
                }

                writer.open();
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                tAccess.reset(buffer);
                int nTuple = tAccess.getTupleCount();
                for (int t = 0; t < nTuple; t++) {
                    tRef.reset(tAccess, t);
                    processTuple(tRef);
                }

            }

            @Override
            public void close() throws HyracksDataException {
                computeAggregate();
                appendToFrameFromTupleBuilder(tupleBuilder);
                super.close();
            }

            private void computeAggregate() throws HyracksDataException {
                tupleBuilder.reset();
                for (int f = 0; f < aggregs.length; f++) {
                    try {
                        aggregs[f].finish(result);
                    } catch (AlgebricksException e) {
                        throw new HyracksDataException(e);
                    }
                    tupleBuilder.addField(result.getByteArray(), result.getStartOffset(), result.getLength());
                }
            }

            private void processTuple(FrameTupleReference tupleRef) throws HyracksDataException {
                for (int f = 0; f < aggregs.length; f++) {
                    try {
                        aggregs[f].step(tupleRef);
                    } catch (AlgebricksException e) {
                        throw new HyracksDataException(e);
                    }
                }
            }

            @Override
            public void fail() throws HyracksDataException {
                writer.fail();
            }
        };
    }
}
