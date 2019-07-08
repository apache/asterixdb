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
package org.apache.hyracks.algebricks.runtime.evaluators;

import org.apache.hyracks.algebricks.common.exceptions.NotImplementedException;
import org.apache.hyracks.algebricks.data.IBinaryBooleanInspector;
import org.apache.hyracks.algebricks.data.IBinaryBooleanInspectorFactory;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.ITuplePairComparator;
import org.apache.hyracks.api.dataflow.value.ITuplePairComparatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import org.apache.hyracks.util.annotations.CriticalPath;

public class TuplePairEvaluatorFactory implements ITuplePairComparatorFactory {

    private static final long serialVersionUID = 1L;
    private final IScalarEvaluatorFactory condition;
    private final IBinaryBooleanInspectorFactory booleanInspectorFactory;
    private final boolean tuplesAreReversed; // whether input tuples are in reverse order of the one in join condition

    public TuplePairEvaluatorFactory(IScalarEvaluatorFactory condition, boolean tuplesAreReversed,
            IBinaryBooleanInspectorFactory booleanInspectorFactory) {
        this.condition = condition;
        this.booleanInspectorFactory = booleanInspectorFactory;
        this.tuplesAreReversed = tuplesAreReversed;
    }

    @Override
    public ITuplePairComparator createTuplePairComparator(IHyracksTaskContext ctx) throws HyracksDataException {
        IEvaluatorContext evalCtx = new EvaluatorContext(ctx);
        IBinaryBooleanInspector bbi = booleanInspectorFactory.createBinaryBooleanInspector(ctx);
        return new TuplePairEvaluator(evalCtx, condition, bbi, tuplesAreReversed);
    }

    private static class TuplePairEvaluator implements ITuplePairComparator {

        private final IScalarEvaluator conditionEvaluator;
        private final IPointable res;
        private final CompositeFrameTupleReference tuplePairRef;
        private final IBinaryBooleanInspector booleanInspector;
        private final Reseter reseter;

        TuplePairEvaluator(IEvaluatorContext ctx, IScalarEvaluatorFactory conditionFactory,
                IBinaryBooleanInspector booleanInspector, boolean tuplesAreReversed) throws HyracksDataException {
            this.conditionEvaluator = conditionFactory.createScalarEvaluator(ctx);
            this.booleanInspector = booleanInspector;
            this.res = VoidPointable.FACTORY.createPointable();
            this.tuplePairRef = new CompositeFrameTupleReference(new FrameTupleReference(), new FrameTupleReference());
            this.reseter = tuplesAreReversed ? TuplePairEvaluator::reverseReset : TuplePairEvaluator::reset;
        }

        @Override
        @CriticalPath
        public int compare(IFrameTupleAccessor leftAccessor, int leftIndex, IFrameTupleAccessor rightAccessor,
                int rightIndex) throws HyracksDataException {
            reseter.reset(tuplePairRef, leftAccessor, leftIndex, rightAccessor, rightIndex);
            conditionEvaluator.evaluate(tuplePairRef, res);
            return booleanInspector.getBooleanValue(res.getByteArray(), res.getStartOffset(), res.getLength()) ? 0 : 1;
        }

        private static void reset(CompositeFrameTupleReference ref, IFrameTupleAccessor leftAccessor,
                int leftTupleIndex, IFrameTupleAccessor rightAccessor, int rightTupleIndex) {
            ref.reset(leftAccessor, leftTupleIndex, rightAccessor, rightTupleIndex);
        }

        private static void reverseReset(CompositeFrameTupleReference ref, IFrameTupleAccessor leftAccessor,
                int leftIndex, IFrameTupleAccessor rightAccessor, int rightIndex) {
            ref.reset(rightAccessor, rightIndex, leftAccessor, leftIndex);
        }
    }

    @FunctionalInterface
    private interface Reseter {
        void reset(CompositeFrameTupleReference ref, IFrameTupleAccessor leftAccessor, int leftTupleIndex,
                IFrameTupleAccessor rightAccessor, int rightTupleIndex);
    }

    private static class CompositeFrameTupleReference implements IFrameTupleReference {

        private final FrameTupleReference refLeft;
        private final FrameTupleReference refRight;

        CompositeFrameTupleReference(FrameTupleReference refLeft, FrameTupleReference refRight) {
            this.refLeft = refLeft;
            this.refRight = refRight;
        }

        private void reset(IFrameTupleAccessor leftAccessor, int leftIndex, IFrameTupleAccessor rightAccessor,
                int rightIndex) {
            refLeft.reset(leftAccessor, leftIndex);
            refRight.reset(rightAccessor, rightIndex);
        }

        @Override
        public int getFieldCount() {
            return refLeft.getFieldCount() + refRight.getFieldCount();
        }

        @Override
        public byte[] getFieldData(int fIdx) {
            int leftFieldCount = refLeft.getFieldCount();
            return fIdx < leftFieldCount ? refLeft.getFieldData(fIdx) : refRight.getFieldData(fIdx - leftFieldCount);
        }

        @Override
        public int getFieldStart(int fIdx) {
            int leftFieldCount = refLeft.getFieldCount();
            return fIdx < leftFieldCount ? refLeft.getFieldStart(fIdx) : refRight.getFieldStart(fIdx - leftFieldCount);
        }

        @Override
        public int getFieldLength(int fIdx) {
            int leftFieldCount = refLeft.getFieldCount();
            return fIdx < leftFieldCount ? refLeft.getFieldLength(fIdx)
                    : refRight.getFieldLength(fIdx - leftFieldCount);
        }

        @Override
        public IFrameTupleAccessor getFrameTupleAccessor() {
            throw new NotImplementedException();
        }

        @Override
        public int getTupleIndex() {
            throw new NotImplementedException();
        }
    }
}
