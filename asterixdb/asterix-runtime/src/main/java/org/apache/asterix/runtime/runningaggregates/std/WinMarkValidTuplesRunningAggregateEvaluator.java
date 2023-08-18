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

import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IWindowAggregateEvaluator;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class WinMarkValidTuplesRunningAggregateEvaluator implements IWindowAggregateEvaluator {

    private final ISerializerDeserializer boolSerde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ABOOLEAN);

    private final IScalarEvaluator[] args;

    private final IPointable[] argFirstValues;

    private final IPointable[] argCurrValues;

    private final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
    private final DataOutput dataOutput = resultStorage.getDataOutput();

    private IBinaryComparator[] argComparators;

    protected boolean first;

    WinMarkValidTuplesRunningAggregateEvaluator(IScalarEvaluator[] args) {
        this.args = args;
        argFirstValues = argCurrValues = new IPointable[args.length];
        for (int i = 0; i < args.length; i++) {
            argFirstValues[i] = new ArrayBackedValueStorage();
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
        boolean value;
        if (first) {
            first = false;
            value = true;
            for (int i = 0; i < args.length; i++) {
                args[i].evaluate(tuple, argFirstValues[i]);
            }
        } else {
            value = sameTuple(tuple);
        }
        resultStorage.reset();
        boolSerde.serialize(ABoolean.valueOf(value), dataOutput);
        result.set(resultStorage);
    }

    private boolean sameTuple(IFrameTupleReference tuple) throws HyracksDataException {
        for (int i = 0; i < args.length; i++) {
            IPointable v1 = argFirstValues[i];
            args[i].evaluate(tuple, argCurrValues[i]);
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