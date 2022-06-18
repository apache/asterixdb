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
import java.io.IOException;
import java.util.Random;

import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.AMutableInt32;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IRunningAggregateEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IRunningAggregateEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

/**
 * See {@code SampleOperationsHelper} for the sampling algorithm details.
 */
public class SampleSlotRunningAggregateFunctionFactory implements IRunningAggregateEvaluatorFactory {

    private static final long serialVersionUID = 2L;

    private final int sampleCardinalityTarget;

    private final long sampleSeed;

    public SampleSlotRunningAggregateFunctionFactory(int sampleCardinalityTarget, long sampleSeed) {
        this.sampleCardinalityTarget = sampleCardinalityTarget;
        this.sampleSeed = sampleSeed;
    }

    @Override
    public IRunningAggregateEvaluator createRunningAggregateEvaluator(IEvaluatorContext ctx)
            throws HyracksDataException {

        int sampleCardinalityTargetPerPartition = getSampleCardinalityTargetPerPartition(sampleCardinalityTarget,
                ctx.getTaskContext().getPartitionCount());

        return new IRunningAggregateEvaluator() {

            private final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
            private final DataOutput resultOutput = resultStorage.getDataOutput();
            @SuppressWarnings("unchecked")
            private final ISerializerDeserializer<AInt32> int32Serde =
                    SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT32);
            private final AMutableInt32 aInt32 = new AMutableInt32(0);

            private final Random rnd = new Random(sampleSeed);
            private long counter;

            @Override
            public void init() {
                counter = 0;
            }

            @Override
            public void step(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
                try {
                    counter++;
                    int outValue = evaluate();

                    resultStorage.reset();
                    aInt32.setValue(outValue);
                    int32Serde.serialize(aInt32, resultOutput);
                    result.set(resultStorage);
                } catch (IOException e) {
                    throw HyracksDataException.create(e);
                }
            }

            private int evaluate() {
                if (counter <= sampleCardinalityTargetPerPartition) {
                    return (int) counter;
                } else {
                    long v = 1 + (long) (rnd.nextDouble() * counter);
                    return v <= sampleCardinalityTargetPerPartition ? (int) v : 0;
                }
            }
        };
    }

    private static int getSampleCardinalityTargetPerPartition(int sampleCardinalityTarget, int nPartitions) {
        return Math.max(1, sampleCardinalityTarget / nPartitions + Math.min(sampleCardinalityTarget % nPartitions, 1));
    }
}
