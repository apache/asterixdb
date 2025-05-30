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

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.hyracks.algebricks.data.IUnnestingPositionWriter;
import org.apache.hyracks.algebricks.data.IUnnestingPositionWriterFactory;
import org.apache.hyracks.algebricks.runtime.base.IUnnestingEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IUnnestingEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.evaluators.EvaluatorContext;
import org.apache.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputOneFramePushRuntime;
import org.apache.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputRuntimeFactory;
import org.apache.hyracks.api.context.IEvaluatorContext;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IMissingWriter;
import org.apache.hyracks.api.dataflow.value.IMissingWriterFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ByteArrayAccessibleOutputStream;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;

public class UnnestRuntimeFactory extends AbstractOneInputOneOutputRuntimeFactory {

    /**
     * @param projection[i] is the input field index of the i-th field in the output tuple
     * @param projection[i] should be @param outCol if i-th field is the unnest field
     * @param projection[i] should be @param positionalCol if i-th field is the positional field
     * @param outCol,positionalCol can be -1 if the output tuple does not contain the unnest/positional field
     * @param outCol,positionalCol should not be in the input field index range to avoid ambiguity
     */

    private static final long serialVersionUID = 1L;

    protected final int outCol;
    protected final int positionalCol;
    private final IUnnestingEvaluatorFactory unnestingFactory;
    private final IUnnestingPositionWriterFactory positionWriterFactory;
    private final boolean leftOuter;
    private final IMissingWriterFactory missingWriterFactory;

    public UnnestRuntimeFactory(int outCol, IUnnestingEvaluatorFactory unnestingFactory, int[] projectionList,
            boolean leftOuter, IMissingWriterFactory missingWriterFactory) {
        this(outCol, -1, unnestingFactory, projectionList, null, leftOuter, missingWriterFactory);
    }

    public UnnestRuntimeFactory(int outCol, int positionalCol, IUnnestingEvaluatorFactory unnestingFactory,
            int[] projectionList, IUnnestingPositionWriterFactory positionWriterFactory, boolean leftOuter,
            IMissingWriterFactory missingWriterFactory) {
        super(projectionList);
        this.outCol = outCol;
        this.positionalCol = positionalCol;
        this.unnestingFactory = unnestingFactory;
        this.positionWriterFactory = positionWriterFactory;
        this.leftOuter = leftOuter;
        this.missingWriterFactory = missingWriterFactory;
    }

    public int[] getProjectionList() {
        return projectionList;
    }

    public int getOutCol() {
        return outCol;
    }

    public int getPositionalCol() {
        return positionalCol;
    }

    public IUnnestingEvaluatorFactory getUnnestingFactory() {
        return unnestingFactory;
    }

    public IUnnestingPositionWriterFactory getPositionWriterFactory() {
        return positionWriterFactory;
    }

    @Override
    public String toString() {
        return "unnest " + outCol + (positionalCol >= 0 ? " at " + positionalCol : "") + " <- " + unnestingFactory
                + " project: " + Arrays.toString(projectionList);
    }

    @Override
    public AbstractOneInputOneOutputOneFramePushRuntime createOneOutputPushRuntime(final IHyracksTaskContext ctx)
            throws HyracksDataException {
        return new UnnestPushRuntime(ctx);
    }

    public class UnnestPushRuntime extends AbstractOneInputOneOutputOneFramePushRuntime {
        private IPointable p = VoidPointable.FACTORY.createPointable();
        protected ArrayTupleBuilder tupleBuilder;
        protected IUnnestingEvaluator unnest;
        private final IUnnestingPositionWriter positionWriter;
        private final IHyracksTaskContext ctx;
        protected ByteArrayAccessibleOutputStream missingBytes;

        public UnnestPushRuntime(IHyracksTaskContext ctx) throws HyracksDataException {
            this.ctx = ctx;
            IEvaluatorContext evalCtx = new EvaluatorContext(ctx);
            unnest = unnestingFactory.createUnnestingEvaluator(evalCtx);
            tupleBuilder = new ArrayTupleBuilder(projectionList.length);
            positionWriter =
                    positionWriterFactory != null ? positionWriterFactory.createUnnestingPositionWriter() : null;
            missingBytes = leftOuter ? writeMissingBytes() : null;
        }

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

        protected void unnesting(int t) throws IOException {
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

        private void writeOutput(int t, int positionIndex, boolean missing) throws HyracksDataException, IOException {
            tupleBuilder.reset();
            for (int f = 0; f < projectionList.length; f++) {
                int col = projectionList[f];
                if (col == outCol) {
                    if (missing) {
                        tupleBuilder.addField(missingBytes.getByteArray(), 0, missingBytes.size());
                    } else {
                        tupleBuilder.addField(p.getByteArray(), p.getStartOffset(), p.getLength());
                    }
                } else if (col == positionalCol) {
                    if (missing) {
                        tupleBuilder.addField(missingBytes.getByteArray(), 0, missingBytes.size());
                    } else {
                        positionWriter.write(tupleBuilder.getDataOutput(), positionIndex);
                        tupleBuilder.addFieldEndOffset();
                    }
                } else {
                    tupleBuilder.addField(tAccess, t, projectionList[f]);
                }
            }
            appendToFrameFromTupleBuilder(tupleBuilder);
        }

        @Override
        public void flush() throws HyracksDataException {
            appender.flush(writer);
        }
    };

    protected ByteArrayAccessibleOutputStream writeMissingBytes() throws HyracksDataException {
        ByteArrayAccessibleOutputStream baos = new ByteArrayAccessibleOutputStream();
        IMissingWriter missingWriter = missingWriterFactory.createMissingWriter();
        missingWriter.writeMissing(new DataOutputStream(baos));
        return baos;
    }
}