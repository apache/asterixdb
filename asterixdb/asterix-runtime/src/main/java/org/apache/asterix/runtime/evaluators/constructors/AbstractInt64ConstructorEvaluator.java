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
import org.apache.asterix.om.base.ADouble;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.AMutableDouble;
import org.apache.asterix.om.base.AMutableInt64;
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

public abstract class AbstractInt64ConstructorEvaluator extends AbstractIntConstructorEvaluator {

    protected final AMutableInt64 aInt64 = new AMutableInt64(0);
    @SuppressWarnings("unchecked")
    protected final ISerializerDeserializer<AInt64> int64Serde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT64);

    private final AMutableDouble aDouble = new AMutableDouble(0);
    @SuppressWarnings("unchecked")
    private final ISerializerDeserializer<ADouble> doubleSerdeNonTagged =
            SerializerDeserializerProvider.INSTANCE.getNonTaggedSerializerDeserializer(BuiltinType.ADOUBLE);

    protected final UTF8StringPointable utf8Ptr = new UTF8StringPointable();

    private final MutableBoolean maybeNumeric = new MutableBoolean();

    protected AbstractInt64ConstructorEvaluator(IEvaluatorContext ctx, IScalarEvaluator inputEval,
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
            case BIGINT:
                result.set(inputArg);
                break;
            case TINYINT:
            case SMALLINT:
            case INTEGER:
                promoteNumeric(inputType, bytes, startOffset + 1, len - 1, result);
                break;
            case FLOAT:
            case DOUBLE:
                demoteNumeric(inputType, bytes, startOffset + 1, len - 1, result);
                break;
            case BOOLEAN:
                resultStorage.reset();
                boolean b = ABooleanSerializerDeserializer.getBoolean(bytes, startOffset + 1);
                aInt64.setValue(b ? 1 : 0);
                int64Serde.serialize(aInt64, out);
                result.set(resultStorage);
                break;
            case STRING:
                utf8Ptr.set(bytes, startOffset + 1, len - 1);
                if (NumberUtils.parseInt64(utf8Ptr, aInt64, maybeNumeric)) {
                    resultStorage.reset();
                    int64Serde.serialize(aInt64, out);
                    result.set(resultStorage);
                } else if (maybeNumeric.booleanValue() && NumberUtils.parseDouble(utf8Ptr, aDouble)) {
                    tmpStorage.reset();
                    doubleSerdeNonTagged.serialize(aDouble, tmpOut);
                    demoteNumeric(ATypeTag.DOUBLE, tmpStorage.getByteArray(), tmpStorage.getStartOffset(),
                            tmpStorage.getLength(), result);
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
    protected BuiltinType getTargetType() {
        return BuiltinType.AINT64;
    }
}
