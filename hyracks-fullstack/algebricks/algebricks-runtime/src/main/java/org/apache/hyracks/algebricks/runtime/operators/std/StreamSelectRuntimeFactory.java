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

import java.io.DataOutput;
import java.nio.ByteBuffer;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.data.IBinaryBooleanInspector;
import org.apache.hyracks.algebricks.data.IBinaryBooleanInspectorFactory;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputOneFieldFramePushRuntime;
import org.apache.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputOneFramePushRuntime;
import org.apache.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputRuntimeFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IMissingWriter;
import org.apache.hyracks.api.dataflow.value.IMissingWriterFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;

public class StreamSelectRuntimeFactory extends AbstractOneInputOneOutputRuntimeFactory {

    private static final long serialVersionUID = 1L;
    // Final
    private final IScalarEvaluatorFactory cond;
    private final IBinaryBooleanInspectorFactory binaryBooleanInspectorFactory;
    private final IMissingWriterFactory missingWriterFactory;
    // Mutable
    private boolean retainMissing;
    private int missingPlaceholderVariableIndex;

    /**
     * @param cond
     * @param projectionList
     *            if projectionList is null, then no projection is performed
     * @param retainMissing
     * @param missingPlaceholderVariableIndex
     * @param missingWriterFactory
     * @throws HyracksDataException
     */
    public StreamSelectRuntimeFactory(IScalarEvaluatorFactory cond, int[] projectionList,
            IBinaryBooleanInspectorFactory binaryBooleanInspectorFactory, boolean retainMissing,
            int missingPlaceholderVariableIndex, IMissingWriterFactory missingWriterFactory) {
        super(projectionList);
        this.cond = cond;
        this.binaryBooleanInspectorFactory = binaryBooleanInspectorFactory;
        this.retainMissing = retainMissing;
        this.missingPlaceholderVariableIndex = missingPlaceholderVariableIndex;
        this.missingWriterFactory = missingWriterFactory;
    }

    public void retainMissing(boolean retainMissing, int index) {
        this.retainMissing = retainMissing;
        this.missingPlaceholderVariableIndex = index;
    }

    @Override
    public String toString() {
        return "stream-select " + cond.toString();
    }

    @Override
    public AbstractOneInputOneOutputOneFramePushRuntime createOneOutputPushRuntime(final IHyracksTaskContext ctx) {
        final IBinaryBooleanInspector bbi = binaryBooleanInspectorFactory.createBinaryBooleanInspector(ctx);
        return new AbstractOneInputOneOutputOneFieldFramePushRuntime() {
            private IPointable p = VoidPointable.FACTORY.createPointable();
            private IScalarEvaluator eval;
            private IMissingWriter missingWriter = null;
            private ArrayTupleBuilder missingTupleBuilder = null;
            private boolean isOpen = false;

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
                isOpen = true;
                writer.open();

                //prepare nullTupleBuilder
                if (retainMissing && missingWriter == null) {
                    missingWriter = missingWriterFactory.createMissingWriter();
                    missingTupleBuilder = new ArrayTupleBuilder(1);
                    DataOutput out = missingTupleBuilder.getDataOutput();
                    missingWriter.writeMissing(out);
                    missingTupleBuilder.addFieldEndOffset();
                }
            }

            @Override
            public void fail() throws HyracksDataException {
                if (isOpen) {
                    super.fail();
                }
            }

            @Override
            public void close() throws HyracksDataException {
                if (isOpen) {
                    try {
                        flushIfNotFailed();
                    } finally {
                        writer.close();
                    }
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
                        if (retainMissing) {
                            for (int i = 0; i < tRef.getFieldCount(); i++) {
                                if (i == missingPlaceholderVariableIndex) {
                                    appendField(missingTupleBuilder.getByteArray(), 0, missingTupleBuilder.getSize());
                                } else {
                                    appendField(tAccess, t, i);
                                }
                            }
                        }
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
