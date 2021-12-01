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

import org.apache.asterix.dataflow.data.nontagged.serde.ABooleanSerializerDeserializer;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.AFloat;
import org.apache.asterix.om.base.AInt8;
import org.apache.asterix.om.base.AMutableFloat;
import org.apache.asterix.om.base.AMutableInt8;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.runtime.evaluators.common.NumberUtils;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;

public abstract class AbstractInt8ConstructorEvaluator extends AbstractIntConstructorEvaluator {

    private final AMutableInt8 aInt8 = new AMutableInt8((byte) 0);
    @SuppressWarnings("unchecked")
    private final ISerializerDeserializer<AInt8> int8Serde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT8);

    private final AMutableFloat aFloat = new AMutableFloat(0);
    @SuppressWarnings("unchecked")
    private final ISerializerDeserializer<AFloat> floatSerdeNonTagged =
            SerializerDeserializerProvider.INSTANCE.getNonTaggedSerializerDeserializer(BuiltinType.AFLOAT);

    private final UTF8StringPointable utf8Ptr = new UTF8StringPointable();

    private final MutableBoolean maybeNumeric = new MutableBoolean();

    protected AbstractInt8ConstructorEvaluator(IEvaluatorContext ctx, IScalarEvaluator inputEval,
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
            case TINYINT:
                result.set(inputArg);
                break;
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
                demoteNumeric(inputType, bytes, startOffset + 1, len - 1, result);
                break;
            case BOOLEAN:
                boolean b = ABooleanSerializerDeserializer.getBoolean(bytes, startOffset + 1);
                aInt8.setValue((byte) (b ? 1 : 0));
                resultStorage.reset();
                int8Serde.serialize(aInt8, out);
                result.set(resultStorage);
                break;
            case STRING:
                utf8Ptr.set(bytes, startOffset + 1, len - 1);
                if (NumberUtils.parseInt8(utf8Ptr, aInt8, maybeNumeric)) {
                    resultStorage.reset();
                    int8Serde.serialize(aInt8, out);
                    result.set(resultStorage);
                } else if (maybeNumeric.booleanValue() && NumberUtils.parseFloat(utf8Ptr, aFloat)) {
                    tmpStorage.reset();
                    floatSerdeNonTagged.serialize(aFloat, tmpOut);
                    demoteNumeric(ATypeTag.FLOAT, tmpStorage.getByteArray(), tmpStorage.getStartOffset(),
                            tmpStorage.getLength(), result);
                    break;
                } else {
                    handleParseError(utf8Ptr, result);
                }
                break;
            default:
                handleUnsupportedType(inputType, result);
                break;
        }
    }

    @Override
    protected final BuiltinType getTargetType() {
        return BuiltinType.AINT8;
    }
}
