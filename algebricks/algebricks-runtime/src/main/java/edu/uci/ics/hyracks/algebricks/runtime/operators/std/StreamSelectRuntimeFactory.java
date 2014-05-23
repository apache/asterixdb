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

import java.io.DataOutput;
import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.data.IBinaryBooleanInspector;
import edu.uci.ics.hyracks.algebricks.data.IBinaryBooleanInspectorFactory;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputOneFramePushRuntime;
import edu.uci.ics.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputRuntimeFactory;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.INullWriter;
import edu.uci.ics.hyracks.api.dataflow.value.INullWriterFactory;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.api.IPointable;
import edu.uci.ics.hyracks.data.std.primitive.VoidPointable;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;

public class StreamSelectRuntimeFactory extends AbstractOneInputOneOutputRuntimeFactory {

    private static final long serialVersionUID = 1L;

    private IScalarEvaluatorFactory cond;

    private IBinaryBooleanInspectorFactory binaryBooleanInspectorFactory;

    private boolean retainNull;

    private int nullPlaceholderVariableIndex;

    private INullWriterFactory nullWriterFactory;

    /**
     * @param cond
     * @param projectionList
     *            if projectionList is null, then no projection is performed
     * @param retainNull
     * @param nullPlaceholderVariableIndex
     * @param nullWriterFactory
     * @throws HyracksDataException
     */
    public StreamSelectRuntimeFactory(IScalarEvaluatorFactory cond, int[] projectionList,
            IBinaryBooleanInspectorFactory binaryBooleanInspectorFactory, boolean retainNull,
            int nullPlaceholderVariableIndex, INullWriterFactory nullWriterFactory) {
        super(projectionList);
        this.cond = cond;
        this.binaryBooleanInspectorFactory = binaryBooleanInspectorFactory;
        this.retainNull = retainNull;
        this.nullPlaceholderVariableIndex = nullPlaceholderVariableIndex;
        this.nullWriterFactory = nullWriterFactory;
    }

    @Override
    public String toString() {
        return "stream-select " + cond.toString();
    }

    @Override
    public AbstractOneInputOneOutputOneFramePushRuntime createOneOutputPushRuntime(final IHyracksTaskContext ctx) {
        final IBinaryBooleanInspector bbi = binaryBooleanInspectorFactory.createBinaryBooleanInspector(ctx);
        return new AbstractOneInputOneOutputOneFramePushRuntime() {
            private IPointable p = VoidPointable.FACTORY.createPointable();
            private IScalarEvaluator eval;
            private INullWriter nullWriter = null;
            private ArrayTupleBuilder nullTupleBuilder = null;

            @Override
            public void open() throws HyracksDataException {
                if (eval == null) {
                    initAccessAppendFieldRef(ctx);
                    try {
                        eval = cond.createScalarEvaluator(ctx);
                    } catch (AlgebricksException ae) {
                        throw new HyracksDataException(ae);
                    }
                }
                writer.open();

                //prepare nullTupleBuilder
                if (retainNull && nullWriter == null) {
                    nullWriter = nullWriterFactory.createNullWriter();
                    nullTupleBuilder = new ArrayTupleBuilder(1);
                    DataOutput out = nullTupleBuilder.getDataOutput();
                    nullWriter.writeNull(out);
                    nullTupleBuilder.addFieldEndOffset();
                }
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                tAccess.reset(buffer);
                int nTuple = tAccess.getTupleCount();
                for (int t = 0; t < nTuple; t++) {
                    tRef.reset(tAccess, t);
                    try {
                        eval.evaluate(tRef, p);
                    } catch (AlgebricksException ae) {
                        throw new HyracksDataException(ae);
                    }
                    if (bbi.getBooleanValue(p.getByteArray(), p.getStartOffset(), p.getLength())) {
                        if (projectionList != null) {
                            appendProjectionToFrame(t, projectionList);
                        } else {
                            appendTupleToFrame(t);
                        }
                    } else {
                        if (retainNull) {
                            //keep all field values as is except setting nullPlaceholderVariable field to null
                            int i = 0;
                            int tryCount = 0;
                            while (true) {
                                for (i = 0; i < tRef.getFieldCount(); i++) {
                                    if (i == nullPlaceholderVariableIndex) {
                                        if (!appender.appendField(nullTupleBuilder.getByteArray(), 0,
                                                nullTupleBuilder.getSize())) {
                                            FrameUtils.flushFrame(frame, writer);
                                            appender.reset(frame, true);
                                            break;
                                        }
                                    } else {
                                        if (!appender.appendField(tAccess, t, i)) {
                                            FrameUtils.flushFrame(frame, writer);
                                            appender.reset(frame, true);
                                            break;
                                        }
                                    }
                                }

                                if (i == tRef.getFieldCount()) {
                                    break;
                                } else {
                                    tryCount++;
                                    if (tryCount == 2) {
                                        throw new IllegalStateException(
                                                "Could not write frame (AbstractOneInputOneOutputOneFramePushRuntime).");
                                    }
                                }
                            }
                        }
                    }
                }
            }
        };
    }

}
