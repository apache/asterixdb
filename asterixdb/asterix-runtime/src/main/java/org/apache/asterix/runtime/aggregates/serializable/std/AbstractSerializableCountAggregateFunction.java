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

import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.AMutableInt64;
import org.apache.asterix.om.base.ANull;
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
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

/**
 * count(NULL) returns NULL.
 */
public abstract class AbstractSerializableCountAggregateFunction extends AbstractSerializableAggregateFunction {
    private static final int MET_NULL_OFFSET = 0;
    private static final int COUNT_OFFSET = 1;

    private AMutableInt64 result = new AMutableInt64(-1);
    @SuppressWarnings("unchecked")
    private ISerializerDeserializer<AInt64> int64Serde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT64);
    @SuppressWarnings("unchecked")
    private ISerializerDeserializer<ANull> nullSerde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ANULL);
    private IPointable inputVal = new VoidPointable();
    private IScalarEvaluator eval;

    public AbstractSerializableCountAggregateFunction(IScalarEvaluatorFactory[] args, IEvaluatorContext context,
            SourceLocation sourceLoc) throws HyracksDataException {
        super(sourceLoc);
        eval = args[0].createScalarEvaluator(context);
    }

    @Override
    public void init(DataOutput state) throws HyracksDataException {
        try {
            state.writeBoolean(false);
            state.writeLong(0);
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public void step(IFrameTupleReference tuple, byte[] state, int start, int len) throws HyracksDataException {
        boolean metNull = BufferSerDeUtil.getBoolean(state, start);
        long cnt = BufferSerDeUtil.getLong(state, start + 1);
        eval.evaluate(tuple, inputVal);
        ATypeTag typeTag =
                EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(inputVal.getByteArray()[inputVal.getStartOffset()]);
        if (typeTag == ATypeTag.MISSING || typeTag == ATypeTag.NULL) {
            processNull(state, start);
        } else {
            cnt++;
        }
        BufferSerDeUtil.writeBoolean(metNull, state, start + MET_NULL_OFFSET);
        BufferSerDeUtil.writeLong(cnt, state, start + COUNT_OFFSET);
    }

    @Override
    public void finish(byte[] state, int start, int len, DataOutput out) throws HyracksDataException {
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
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public void finishPartial(byte[] state, int start, int len, DataOutput out) throws HyracksDataException {
        finish(state, start, len, out);
    }

    protected void processNull(byte[] state, int start) {
        BufferSerDeUtil.writeBoolean(true, state, start + MET_NULL_OFFSET);
    }
}
