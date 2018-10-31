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

import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.AMutableInt64;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IWindowAggregateEvaluator;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

/**
 * Evaluator {@code ntile()} window function
 */
public class NtileRunningAggregateEvaluator implements IWindowAggregateEvaluator {

    private final IScalarEvaluator evalNumGroups;

    private final VoidPointable argNumGroups = VoidPointable.FACTORY.createPointable();

    private final FunctionIdentifier funId;

    @SuppressWarnings("unchecked")
    private final ISerializerDeserializer<AInt64> serde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT64);

    private final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();

    private final AMutableInt64 aInt64 = new AMutableInt64(0);

    private long partitionLength;

    private long groupSize;

    private long groupRemainder;

    private long resultValue;

    private long count;

    NtileRunningAggregateEvaluator(IScalarEvaluator evalNumGroups, FunctionIdentifier funId) {
        this.evalNumGroups = evalNumGroups;
        this.funId = funId;
    }

    @Override
    public void init() throws HyracksDataException {
    }

    @Override
    public void initPartition(long partitionLength) {
        this.partitionLength = partitionLength;
        resultValue = 0;
    }

    @Override
    public void step(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
        if (resultValue == 0) {
            evaluateGroupSize(tuple);
            resultValue = count = 1;
        } else if (count < groupSize) {
            count++;
        } else if (count == groupSize && groupRemainder > 0) {
            groupRemainder--;
            count++;
        } else {
            resultValue++;
            count = 1;
        }

        resultStorage.reset();
        aInt64.setValue(resultValue);
        serde.serialize(aInt64, resultStorage.getDataOutput());
        result.set(resultStorage);
    }

    private void evaluateGroupSize(IFrameTupleReference tuple) throws HyracksDataException {
        evalNumGroups.evaluate(tuple, argNumGroups);
        byte[] bytes = argNumGroups.getByteArray();
        int offset = argNumGroups.getStartOffset();
        long numGroups = ATypeHierarchy.getLongValue(funId.getName(), 0, bytes, offset);
        if (numGroups > partitionLength || numGroups <= 0) {
            groupSize = partitionLength;
            groupRemainder = 0;
        } else {
            groupSize = partitionLength / numGroups;
            groupRemainder = partitionLength % numGroups;
        }
    }
}
