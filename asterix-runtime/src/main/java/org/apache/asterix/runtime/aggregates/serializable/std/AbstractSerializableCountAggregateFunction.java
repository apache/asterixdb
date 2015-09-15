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
package org.apache.asterix.runtime.aggregates.serializable.std;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.AMutableInt64;
import org.apache.asterix.om.base.ANull;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluator;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.base.ICopySerializableAggregateFunction;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

/**
 * count(NULL) returns NULL.
 */
public abstract class AbstractSerializableCountAggregateFunction implements ICopySerializableAggregateFunction {
    private static final int MET_NULL_OFFSET = 0;
    private static final int COUNT_OFFSET = 1;

    private AMutableInt64 result = new AMutableInt64(-1);
    @SuppressWarnings("unchecked")
    private ISerializerDeserializer<AInt64> int64Serde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.AINT64);
    @SuppressWarnings("unchecked")
    private ISerializerDeserializer<ANull> nullSerde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.ANULL);
    private ArrayBackedValueStorage inputVal = new ArrayBackedValueStorage();
    private ICopyEvaluator eval;

    public AbstractSerializableCountAggregateFunction(ICopyEvaluatorFactory[] args) throws AlgebricksException {
        eval = args[0].createEvaluator(inputVal);
    }

    @Override
    public void init(DataOutput state) throws AlgebricksException {
        try {
            state.writeBoolean(false);
            state.writeLong(0);
        } catch (IOException e) {
            throw new AlgebricksException(e);
        }
    }

    @Override
    public void step(IFrameTupleReference tuple, byte[] state, int start, int len) throws AlgebricksException {
        boolean metNull = BufferSerDeUtil.getBoolean(state, start);
        long cnt = BufferSerDeUtil.getLong(state, start + 1);
        inputVal.reset();
        eval.evaluate(tuple);
        ATypeTag typeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(inputVal.getByteArray()[0]);
        if (typeTag == ATypeTag.NULL) {
            processNull(state, start);
        } else {
            cnt++;
        }
        BufferSerDeUtil.writeBoolean(metNull, state, start + MET_NULL_OFFSET);
        BufferSerDeUtil.writeLong(cnt, state, start + COUNT_OFFSET);
    }

    @Override
    public void finish(byte[] state, int start, int len, DataOutput out) throws AlgebricksException {
        boolean metNull = BufferSerDeUtil.getBoolean(state, start);
        long cnt = BufferSerDeUtil.getLong(state, start + 1);
        try {
            if (metNull) {
                nullSerde.serialize(ANull.NULL, out);
            } else {
                result.setValue(cnt);
                int64Serde.serialize(result, out);
            }
        } catch (IOException e) {
            throw new AlgebricksException(e);
        }
    }

    @Override
    public void finishPartial(byte[] state, int start, int len, DataOutput out) throws AlgebricksException {
        finish(state, start, len, out);
    }

    protected void processNull(byte[] state, int start) {
        BufferSerDeUtil.writeBoolean(true, state, start + MET_NULL_OFFSET);
    }
}
