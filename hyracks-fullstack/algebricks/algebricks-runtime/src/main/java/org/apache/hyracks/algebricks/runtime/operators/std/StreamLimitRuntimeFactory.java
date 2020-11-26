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

import java.nio.ByteBuffer;

import org.apache.hyracks.algebricks.data.IBinaryIntegerInspector;
import org.apache.hyracks.algebricks.data.IBinaryIntegerInspectorFactory;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.evaluators.EvaluatorContext;
import org.apache.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputOneFramePushRuntime;
import org.apache.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputRuntimeFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;

public class StreamLimitRuntimeFactory extends AbstractOneInputOneOutputRuntimeFactory {

    private static final long serialVersionUID = 1L;

    private final IScalarEvaluatorFactory maxObjectsEvalFactory;
    private final IScalarEvaluatorFactory offsetEvalFactory;
    private final IBinaryIntegerInspectorFactory binaryIntegerInspectorFactory;

    public StreamLimitRuntimeFactory(IScalarEvaluatorFactory maxObjectsEvalFactory,
            IScalarEvaluatorFactory offsetEvalFactory, int[] projectionList,
            IBinaryIntegerInspectorFactory binaryIntegerInspectorFactory) {
        super(projectionList);
        if (maxObjectsEvalFactory == null && offsetEvalFactory == null) {
            throw new IllegalArgumentException();
        }
        this.maxObjectsEvalFactory = maxObjectsEvalFactory;
        this.offsetEvalFactory = offsetEvalFactory;
        this.binaryIntegerInspectorFactory = binaryIntegerInspectorFactory;
    }

    @Override
    public String toString() {
        String s = "stream-limit " + maxObjectsEvalFactory.toString();
        if (offsetEvalFactory != null) {
            return s + ", " + offsetEvalFactory.toString();
        } else {
            return s;
        }
    }

    @Override
    public AbstractOneInputOneOutputOneFramePushRuntime createOneOutputPushRuntime(final IHyracksTaskContext ctx)
            throws HyracksDataException {
        IEvaluatorContext evalCtx = new EvaluatorContext(ctx);
        final IBinaryIntegerInspector bii = binaryIntegerInspectorFactory.createBinaryIntegerInspector(ctx);
        return new AbstractOneInputOneOutputOneFramePushRuntime() {
            private final IPointable p = VoidPointable.FACTORY.createPointable();
            private final IScalarEvaluator evalMaxObjects =
                    maxObjectsEvalFactory != null ? maxObjectsEvalFactory.createScalarEvaluator(evalCtx) : null;
            private final IScalarEvaluator evalOffset =
                    offsetEvalFactory != null ? offsetEvalFactory.createScalarEvaluator(evalCtx) : null;
            private final boolean toWriteUnlimited = maxObjectsEvalFactory == null;
            private int toWrite; // how many tuples still to write
            private int toSkip; // how many tuples still to skip
            private boolean firstTuple;
            private boolean afterLastTuple;

            @Override
            public void open() throws HyracksDataException {
                super.open();
                if (tRef == null) {
                    initAccessAppendRef(ctx);
                }
                firstTuple = true;
                afterLastTuple = false;
                toWrite = 0;
                toSkip = 0;
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                if (afterLastTuple) {
                    return;
                }
                tAccess.reset(buffer);
                int nTuple = tAccess.getTupleCount();
                int start = 0;
                if (nTuple <= toSkip) {
                    toSkip -= nTuple;
                    return;
                } else if (toSkip > 0) {
                    start = toSkip;
                    toSkip = 0;
                }
                for (int t = start; t < nTuple; t++) {
                    if (firstTuple) {
                        firstTuple = false;
                        if (evalMaxObjects != null) {
                            toWrite = evaluateInteger(evalMaxObjects, t);
                        }
                        if (evalOffset != null) {
                            toSkip = evaluateInteger(evalOffset, t);
                        }
                    }
                    if (toSkip > 0) {
                        toSkip--;
                    } else if (toWriteUnlimited || toWrite > 0) {
                        toWrite--;
                        if (projectionList != null) {
                            appendProjectionToFrame(t, projectionList);
                        } else {
                            appendTupleToFrame(t);
                        }
                    } else {
                        afterLastTuple = true;
                        break;
                    }
                }
            }

            private int evaluateInteger(IScalarEvaluator eval, int tIdx) throws HyracksDataException {
                tRef.reset(tAccess, tIdx);
                eval.evaluate(tRef, p);
                return bii.getIntegerValue(p.getByteArray(), p.getStartOffset(), p.getLength());
            }

            @Override
            public void flush() throws HyracksDataException {
                appender.flush(writer);
            }
        };
    }
}
