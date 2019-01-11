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
import org.apache.hyracks.algebricks.runtime.base.IWindowAggregateEvaluator;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

/**
 * Evaluator for internal {@code win_partition_length()} window function
 */
public class WinPartitionLenRunningAggregateEvaluator implements IWindowAggregateEvaluator {

    @SuppressWarnings("unchecked")
    private final ISerializerDeserializer<AInt64> serde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT64);

    private final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();

    private final AMutableInt64 aInt64 = new AMutableInt64(0);

    @Override
    public void init() {
        // nothing to do
    }

    @Override
    public void initPartition(long partitionLength) throws HyracksDataException {
        resultStorage.reset();
        aInt64.setValue(partitionLength);
        serde.serialize(aInt64, resultStorage.getDataOutput());
    }

    @Override
    public void step(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
        result.set(resultStorage);
    }
}
