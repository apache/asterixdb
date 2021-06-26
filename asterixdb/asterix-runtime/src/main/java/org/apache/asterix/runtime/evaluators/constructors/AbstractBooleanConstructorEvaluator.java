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

package org.apache.asterix.runtime.evaluators.constructors;

import org.apache.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AFloatSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt16SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt64SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt8SerializerDeserializer;
import org.apache.asterix.formats.nontagged.BinaryComparatorFactoryProvider;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.runtime.evaluators.common.NumberUtils;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.util.string.UTF8StringUtil;

public abstract class AbstractBooleanConstructorEvaluator extends AbstractConstructorEvaluator {

    @SuppressWarnings("unchecked")
    protected final ISerializerDeserializer<ABoolean> booleanSerde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ABOOLEAN);

    protected static final IBinaryComparator UTF8_BINARY_CMP =
            BinaryComparatorFactoryProvider.UTF8STRING_POINTABLE_INSTANCE.createBinaryComparator();

    protected static final byte[] TRUE = UTF8StringUtil.writeStringToBytes("true");
    protected static final byte[] FALSE = UTF8StringUtil.writeStringToBytes("false");

    private final UTF8StringPointable utf8Ptr = new UTF8StringPointable();

    protected AbstractBooleanConstructorEvaluator(IEvaluatorContext ctx, IScalarEvaluator inputEval,
            SourceLocation sourceLoc) {
        super(ctx, inputEval, sourceLoc);
    }

    @Override
    protected void evaluateImpl(IPointable result) throws HyracksDataException {
        byte[] bytes = inputArg.getByteArray();
        int startOffset = inputArg.getStartOffset();
        int len = inputArg.getLength();
        ATypeTag inputType = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(bytes[startOffset]);
        switch (inputType) {
            case BOOLEAN:
                result.set(inputArg);
                break;
            case TINYINT:
                setInteger(AInt8SerializerDeserializer.getByte(bytes, startOffset + 1), result);
                break;
            case SMALLINT:
                setInteger(AInt16SerializerDeserializer.getShort(bytes, startOffset + 1), result);
                break;
            case INTEGER:
                setInteger(AInt32SerializerDeserializer.getInt(bytes, startOffset + 1), result);
                break;
            case BIGINT:
                setInteger(AInt64SerializerDeserializer.getLong(bytes, startOffset + 1), result);
                break;
            case FLOAT:
                setDouble(AFloatSerializerDeserializer.getFloat(bytes, startOffset + 1), result);
                break;
            case DOUBLE:
                setDouble(ADoubleSerializerDeserializer.getDouble(bytes, startOffset + 1), result);
                break;
            case STRING:
                Boolean v = parseBoolean(bytes, startOffset, len);
                if (v != null) {
                    setBoolean(result, v);
                } else {
                    utf8Ptr.set(bytes, startOffset + 1, len - 1);
                    handleParseError(utf8Ptr, result);
                }
                break;
            default:
                handleUnsupportedType(inputType, result);
                break;
        }
    }

    protected Boolean parseBoolean(byte[] bytes, int offset, int len) throws HyracksDataException {
        if (UTF8_BINARY_CMP.compare(bytes, offset + 1, len - 1, TRUE, 0, TRUE.length) == 0) {
            return true;
        } else if (UTF8_BINARY_CMP.compare(bytes, offset + 1, len - 1, FALSE, 0, FALSE.length) == 0) {
            return false;
        } else {
            return null;
        }
    }

    protected final void setBoolean(IPointable result, boolean value) throws HyracksDataException {
        resultStorage.reset();
        booleanSerde.serialize(ABoolean.valueOf(value), out);
        result.set(resultStorage);
    }

    protected final void setInteger(long v, IPointable result) throws HyracksDataException {
        setBoolean(result, v != 0);
    }

    protected final void setDouble(double v, IPointable result) throws HyracksDataException {
        long bits = Double.doubleToLongBits(v);
        boolean zeroOrNaN = bits == NumberUtils.POSITIVE_ZERO_BITS || bits == NumberUtils.NEGATIVE_ZERO_BITS
                || bits == NumberUtils.NAN_BITS;
        setBoolean(result, !zeroOrNaN);
    }

    @Override
    protected BuiltinType getTargetType() {
        return BuiltinType.ABOOLEAN;
    }
}
