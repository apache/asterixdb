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

package org.apache.hyracks.dataflow.std.intersect;

import java.nio.ByteBuffer;
import java.util.BitSet;

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.IActivity;
import org.apache.hyracks.api.dataflow.IActivityGraphBuilder;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputer;
import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.std.base.AbstractActivityNode;
import org.apache.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryOutputOperatorNodePushable;

/**
 * This intersection operator is to get the common elements from multiple way inputs.
 * It will only produce the projected fields which are used for comparison.
 */
public class IntersectOperatorDescriptor extends AbstractOperatorDescriptor {

    private static final long serialVersionUID = 1L;
    private final int[][] projectFields;
    private final INormalizedKeyComputerFactory firstKeyNormalizerFactory;
    private final IBinaryComparatorFactory[] comparatorFactory;

    /**
     * @param spec
     * @param nInputs                   Number of inputs
     * @param compareAndProjectFields   The project field list of each input.
     *                                  All the fields order should be the same with the comparatorFactories
     * @param firstKeyNormalizerFactory Normalizer for the first comparison key.
     * @param comparatorFactories       A list of comparators for each field
     * @param recordDescriptor
     * @throws HyracksException
     */
    public IntersectOperatorDescriptor(IOperatorDescriptorRegistry spec, int nInputs, int[][] compareAndProjectFields,
            INormalizedKeyComputerFactory firstKeyNormalizerFactory, IBinaryComparatorFactory[] comparatorFactories,
            RecordDescriptor recordDescriptor) throws HyracksException {
        super(spec, nInputs, 1);
        recordDescriptors[0] = recordDescriptor;

        validateParameters(compareAndProjectFields, comparatorFactories);

        this.projectFields = compareAndProjectFields;
        this.firstKeyNormalizerFactory = firstKeyNormalizerFactory;
        this.comparatorFactory = comparatorFactories;
    }

    private void validateParameters(int[][] compareAndProjectFields, IBinaryComparatorFactory[] comparatorFactories)
            throws HyracksException {

        int firstLength = compareAndProjectFields[0].length;
        for (int[] fields : compareAndProjectFields) {
            if (fields.length != firstLength) {
                throw new HyracksException("The given input comparison fields is not equal");
            }
            for (int fid : fields) {
                if (fid < 0) {
                    throw new HyracksException("Invalid field index in given comparison fields array");
                }
            }
        }

        if (firstLength != comparatorFactories.length) {
            throw new HyracksException("The size of given fields is not equal with the number of comparators");
        }
    }

    @Override
    public void contributeActivities(IActivityGraphBuilder builder) {
        IActivity intersectActivity = new IntersectActivity(new ActivityId(getOperatorId(), 0));
        builder.addActivity(this, intersectActivity);
        for (int i = 0; i < getInputArity(); i++) {
            builder.addSourceEdge(i, intersectActivity, i);
        }
        builder.addTargetEdge(0, intersectActivity, 0);
    }

    private class IntersectActivity extends AbstractActivityNode {

        private static final long serialVersionUID = 1L;

        public IntersectActivity(ActivityId activityId) {
            super(activityId);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions)
                throws HyracksDataException {
            RecordDescriptor[] inputRecordDesc = new RecordDescriptor[inputArity];
            for (int i = 0; i < inputRecordDesc.length; i++) {
                inputRecordDesc[i] = recordDescProvider.getInputRecordDescriptor(getActivityId(), i);
            }
            return new IntersectOperatorNodePushable(ctx, inputArity, inputRecordDesc, projectFields,
                    firstKeyNormalizerFactory, comparatorFactory);
        }
    }

    public static class IntersectOperatorNodePushable extends AbstractUnaryOutputOperatorNodePushable {

        private enum ACTION {FAILED, CLOSE}

        private final int inputArity;
        private final int[][] projectFields;
        private final BitSet consumed;
        private final int[] tupleIndexMarker;
        private final FrameTupleAccessor[] refAccessor;
        private final FrameTupleAppender appender;

        private final INormalizedKeyComputer firstKeyNormalizerComputer;
        private final IBinaryComparator[] comparators;

        private boolean done = false;

        public IntersectOperatorNodePushable(IHyracksTaskContext ctx, int inputArity,
                RecordDescriptor[] inputRecordDescriptors, int[][] projectFields,
                INormalizedKeyComputerFactory firstKeyNormalizerFactory, IBinaryComparatorFactory[] comparatorFactory)
                throws HyracksDataException {

            this.inputArity = inputArity;
            this.projectFields = projectFields;
            this.firstKeyNormalizerComputer =
                    firstKeyNormalizerFactory == null ? null : firstKeyNormalizerFactory.createNormalizedKeyComputer();

            comparators = new IBinaryComparator[projectFields[0].length];
            for (int i = 0; i < comparators.length; i++) {
                comparators[i] = comparatorFactory[i].createBinaryComparator();
            }

            appender = new FrameTupleAppender(new VSizeFrame(ctx));

            refAccessor = new FrameTupleAccessor[inputArity];
            for (int i = 0; i < inputArity; i++) {
                refAccessor[i] = new FrameTupleAccessor(inputRecordDescriptors[i]);
            }

            consumed = new BitSet(inputArity);
            consumed.set(0, inputArity);
            tupleIndexMarker = new int[inputArity];
        }

        @Override
        public int getInputArity() {
            return inputArity;
        }

        @Override
        public IFrameWriter getInputFrameWriter(final int index) {
            return new IFrameWriter() {
                @Override
                public void open() throws HyracksDataException {
                    if (index == 0) {
                        writer.open();
                    }
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    synchronized (IntersectOperatorNodePushable.this) {
                        if (done) {
                            return;
                        }
                        refAccessor[index].reset(buffer);
                        tupleIndexMarker[index] = 0;
                        consumed.clear(index);
                        if (index != 0) {
                            if (allInputArrived()) {
                                IntersectOperatorNodePushable.this.notifyAll();
                            }
                            while (!consumed.get(index) && !done) {
                                waitOrHyracksException();
                            }
                        } else { //(index == 0)
                            while (!consumed.get(0)) {
                                while (!allInputArrived() && !done) {
                                    waitOrHyracksException();
                                }
                                if (done) {
                                    break;
                                }
                                intersectAllInputs();
                                IntersectOperatorNodePushable.this.notifyAll();
                            }
                        }
                    }
                }

                private void waitOrHyracksException() throws HyracksDataException {
                    try {
                        IntersectOperatorNodePushable.this.wait();
                    } catch (InterruptedException e) {
                        throw new HyracksDataException(e);
                    }
                }

                private boolean allInputArrived() {
                    return consumed.cardinality() == 0;
                }

                private void intersectAllInputs() throws HyracksDataException {
                    do {
                        int maxInput = findMaxInput();
                        int match = 1;
                        boolean needToUpdateMax = false;
                        for (int i = 0; i < inputArity; i++) {
                            if (i == maxInput) {
                                continue;
                            }
                            while (tupleIndexMarker[i] < refAccessor[i].getTupleCount()) {
                                int cmp = compare(i, refAccessor[i], tupleIndexMarker[i], maxInput,
                                        refAccessor[maxInput], tupleIndexMarker[maxInput]);
                                if (cmp == 0) {
                                    match++;
                                    break;
                                } else if (cmp < 0) {
                                    tupleIndexMarker[i]++;
                                } else {
                                    needToUpdateMax = true;
                                    break;
                                }
                            }

                            if (tupleIndexMarker[i] >= refAccessor[i].getTupleCount()) {
                                consumed.set(i);
                            }
                        }
                        if (match == inputArity) {
                            FrameUtils.appendProjectionToWriter(writer, appender, refAccessor[maxInput],
                                    tupleIndexMarker[maxInput], projectFields[maxInput]);
                            for (int i = 0; i < inputArity; i++) {
                                tupleIndexMarker[i]++;
                                if (tupleIndexMarker[i] >= refAccessor[i].getTupleCount()) {
                                    consumed.set(i);
                                }
                            }
                        } else if (needToUpdateMax) {
                            tupleIndexMarker[maxInput]++;
                            if (tupleIndexMarker[maxInput] >= refAccessor[maxInput].getTupleCount()) {
                                consumed.set(maxInput);
                            }
                        }

                    } while (consumed.nextSetBit(0) < 0);
                    appender.write(writer, true);
                }

                private int compare(int input1, FrameTupleAccessor frameTupleAccessor1, int tid1, int input2,
                        FrameTupleAccessor frameTupleAccessor2, int tid2) throws HyracksDataException {
                    int firstNorm1 = getFirstNorm(input1, frameTupleAccessor1, tid1);
                    int firstNorm2 = getFirstNorm(input2, frameTupleAccessor2, tid2);

                    if (firstNorm1 < firstNorm2) {
                        return -1;
                    } else if (firstNorm1 > firstNorm2) {
                        return 1;
                    }

                    for (int i = 0; i < comparators.length; i++) {
                        int cmp = comparators[i].compare(frameTupleAccessor1.getBuffer().array(),
                                frameTupleAccessor1.getAbsoluteFieldStartOffset(tid1, projectFields[input1][i]),
                                frameTupleAccessor1.getFieldLength(tid1, projectFields[input1][i]),
                                frameTupleAccessor2.getBuffer().array(),
                                frameTupleAccessor2.getAbsoluteFieldStartOffset(tid2, projectFields[input2][i]),
                                frameTupleAccessor2.getFieldLength(tid2, projectFields[input2][i]));

                        if (cmp != 0) {
                            return cmp;
                        }
                    }
                    return 0;
                }

                private int getFirstNorm(int inputId1, FrameTupleAccessor frameTupleAccessor1, int tid1) {
                    return firstKeyNormalizerComputer == null ?
                            0 :
                            firstKeyNormalizerComputer.normalize(frameTupleAccessor1.getBuffer().array(),
                                    frameTupleAccessor1.getAbsoluteFieldStartOffset(tid1, projectFields[inputId1][0]),
                                    frameTupleAccessor1.getFieldLength(tid1, projectFields[inputId1][0]));
                }

                private int findMaxInput() throws HyracksDataException {
                    int max = 0;
                    for (int i = 1; i < inputArity; i++) {
                        int cmp = compare(max, refAccessor[max], tupleIndexMarker[max], i, refAccessor[i],
                                tupleIndexMarker[i]);
                        if (cmp < 0) {
                            max = i;
                        }
                    }
                    return max;
                }

                @Override
                public void fail() throws HyracksDataException {
                    clearStateWith(ACTION.FAILED);
                }

                @Override
                public void close() throws HyracksDataException {
                    clearStateWith(ACTION.CLOSE);
                }

                private void clearStateWith(ACTION action) throws HyracksDataException {
                    synchronized (IntersectOperatorNodePushable.this) {
                        if (index == 0) {
                            doAction(action);
                        }
                        if (done) {
                            return;
                        }
                        consumed.set(index);
                        refAccessor[index] = null;
                        done = true;
                        IntersectOperatorNodePushable.this.notifyAll();
                    }
                }

                private void doAction(ACTION action) throws HyracksDataException {
                    switch (action) {
                        case CLOSE:
                            writer.close();
                            break;
                        case FAILED:
                            writer.fail();
                            break;
                    }
                }

            };
        }
    }
}
