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

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.data.IBinaryIntegerInspector;
import edu.uci.ics.hyracks.algebricks.data.IBinaryIntegerInspectorFactory;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputOneFramePushRuntime;
import edu.uci.ics.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputRuntimeFactory;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.api.IPointable;
import edu.uci.ics.hyracks.data.std.primitive.VoidPointable;

public class StreamLimitRuntimeFactory extends AbstractOneInputOneOutputRuntimeFactory {

    private static final long serialVersionUID = 1L;

    private IScalarEvaluatorFactory maxObjectsEvalFactory;
    private IScalarEvaluatorFactory offsetEvalFactory;
    private IBinaryIntegerInspectorFactory binaryIntegerInspectorFactory;

    public StreamLimitRuntimeFactory(IScalarEvaluatorFactory maxObjectsEvalFactory,
            IScalarEvaluatorFactory offsetEvalFactory, int[] projectionList,
            IBinaryIntegerInspectorFactory binaryIntegerInspectorFactory) {
        super(projectionList);
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
    public AbstractOneInputOneOutputOneFramePushRuntime createOneOutputPushRuntime(final IHyracksTaskContext ctx) {
        final IBinaryIntegerInspector bii = binaryIntegerInspectorFactory.createBinaryIntegerInspector(ctx);
        return new AbstractOneInputOneOutputOneFramePushRuntime() {
            private IPointable p = VoidPointable.FACTORY.createPointable();
            private IScalarEvaluator evalMaxObjects;
            private IScalarEvaluator evalOffset = null;
            private int toWrite = 0; // how many tuples still to write
            private int toSkip = 0; // how many tuples still to skip
            private boolean firstTuple = true;
            private boolean afterLastTuple = false;

            @Override
            public void open() throws HyracksDataException {
                // if (first) {
                if (evalMaxObjects == null) {
                    initAccessAppendRef(ctx);
                    try {
                        evalMaxObjects = maxObjectsEvalFactory.createScalarEvaluator(ctx);
                        if (offsetEvalFactory != null) {
                            evalOffset = offsetEvalFactory.createScalarEvaluator(ctx);
                        }
                    } catch (AlgebricksException ae) {
                        throw new HyracksDataException(ae);
                    }
                }
                writer.open();
                afterLastTuple = false;
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                if (afterLastTuple) {
                    // ignore the data
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
                        toWrite = evaluateInteger(evalMaxObjects, t);
                        if (evalOffset != null) {
                            toSkip = evaluateInteger(evalOffset, t);
                        }
                    }
                    if (toSkip > 0) {
                        toSkip--;
                    } else if (toWrite > 0) {
                        toWrite--;
                        if (projectionList != null) {
                            appendProjectionToFrame(t, projectionList);
                        } else {
                            appendTupleToFrame(t);
                        }
                    } else {
                        // close();
                        afterLastTuple = true;
                        break;
                    }
                }
            }

            @Override
            public void close() throws HyracksDataException {
                // if (!afterLastTuple) {
                super.close();
                // }
            }

            private int evaluateInteger(IScalarEvaluator eval, int tIdx) throws HyracksDataException {
                tRef.reset(tAccess, tIdx);
                try {
                    eval.evaluate(tRef, p);
                } catch (AlgebricksException ae) {
                    throw new HyracksDataException(ae);
                }
                int lim = bii.getIntegerValue(p.getByteArray(), p.getStartOffset(), p.getLength());
                return lim;
            }

        };
    }

}
