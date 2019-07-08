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
package org.apache.asterix.runtime.aggregates.std;

import java.io.IOException;

import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.AMutableInt64;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

/**
 * COUNT returns the number of items in the given list. Note that COUNT(NULL) is not allowed.
 */
public abstract class AbstractCountAggregateFunction extends AbstractAggregateFunction {
    private AMutableInt64 result = new AMutableInt64(-1);
    @SuppressWarnings("unchecked")
    private ISerializerDeserializer<AInt64> int64Serde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT64);
    private IPointable inputVal = new VoidPointable();
    private IScalarEvaluator eval;
    protected long cnt;

    private ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();

    public AbstractCountAggregateFunction(IScalarEvaluatorFactory[] args, IEvaluatorContext context,
            SourceLocation sourceLoc) throws HyracksDataException {
        super(sourceLoc);
        eval = args[0].createScalarEvaluator(context);
    }

    @Override
    public void init() throws HyracksDataException {
        cnt = 0;
    }

    @Override
    public void step(IFrameTupleReference tuple) throws HyracksDataException {
        eval.evaluate(tuple, inputVal);
        ATypeTag typeTag =
                EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(inputVal.getByteArray()[inputVal.getStartOffset()]);
        // Ignore SYSTEM_NULL.
        if (typeTag == ATypeTag.NULL || typeTag == ATypeTag.MISSING) {
            processNull();
        } else if (typeTag != ATypeTag.SYSTEM_NULL) {
            cnt++;
        }
    }

    @Override
    public void finish(IPointable resultPointable) throws HyracksDataException {
        resultStorage.reset();
        try {
            result.setValue(cnt);
            int64Serde.serialize(result, resultStorage.getDataOutput());
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
        resultPointable.set(resultStorage);
    }

    @Override
    public void finishPartial(IPointable resultPointable) throws HyracksDataException {
        finish(resultPointable);
    }

    protected abstract void processNull();
}
