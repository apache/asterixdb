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

package org.apache.asterix.runtime.runningaggregates.std;

import java.io.DataOutput;

import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IWindowAggregateEvaluator;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

/**
 * Base evaluator implementation for ranking window functions:
 * {@code rank()}, {@code dense_rank()}, {@code percent_rank()}
 */
public abstract class AbstractRankRunningAggregateEvaluator implements IWindowAggregateEvaluator {

    private final IScalarEvaluator[] args;

    private final ArrayBackedValueStorage[] argPrevValues;

    private final IPointable[] argCurrValues;

    private final boolean dense;

    protected final SourceLocation sourceLoc;

    private final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();

    private IBinaryComparator[] argComparators;

    protected boolean first;

    protected long rank;

    private long groupSize;

    AbstractRankRunningAggregateEvaluator(IScalarEvaluator[] args, boolean dense, SourceLocation sourceLoc) {
        this.args = args;
        this.dense = dense;
        this.sourceLoc = sourceLoc;
        argPrevValues = new ArrayBackedValueStorage[args.length];
        argCurrValues = new IPointable[args.length];
        for (int i = 0; i < args.length; i++) {
            argPrevValues[i] = new ArrayBackedValueStorage();
            argCurrValues[i] = VoidPointable.FACTORY.createPointable();
        }
    }

    @Override
    public void configure(IBinaryComparator[] orderComparators) {
        argComparators = orderComparators;
    }

    @Override
    public void init() throws HyracksDataException {
    }

    @Override
    public void initPartition(long partitionLength) {
        first = true;
    }

    @Override
    public void step(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
        resultStorage.reset();
        for (int i = 0; i < args.length; i++) {
            args[i].evaluate(tuple, argCurrValues[i]);
        }

        computeRank();
        computeResult(resultStorage.getDataOutput());

        result.set(resultStorage);

        for (int i = 0; i < args.length; i++) {
            argPrevValues[i].assign(argCurrValues[i]);
        }
        first = false;
    }

    protected abstract void computeResult(DataOutput out) throws HyracksDataException;

    private void computeRank() throws HyracksDataException {
        if (first) {
            rank = 1;
            groupSize = 1;
        } else if (sameGroup()) {
            groupSize++;
        } else {
            rank += dense ? 1 : groupSize;
            groupSize = 1;
        }
    }

    private boolean sameGroup() throws HyracksDataException {
        for (int i = 0; i < args.length; i++) {
            IPointable v1 = argPrevValues[i];
            IPointable v2 = argCurrValues[i];
            IBinaryComparator cmp = argComparators[i];
            if (cmp.compare(v1.getByteArray(), v1.getStartOffset(), v1.getLength(), v2.getByteArray(),
                    v2.getStartOffset(), v2.getLength()) != 0) {
                return false;
            }
        }
        return true;
    }
}
