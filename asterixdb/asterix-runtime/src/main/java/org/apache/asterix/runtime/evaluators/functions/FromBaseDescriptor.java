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

package org.apache.asterix.runtime.evaluators.functions;

import org.apache.asterix.common.annotations.MissingNullInOutFunction;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.AMutableInt32;
import org.apache.asterix.om.base.AMutableInt64;
import org.apache.asterix.om.exceptions.ExceptionUtil;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.evaluators.common.ArgumentUtils;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IEvaluatorContext;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import org.apache.hyracks.util.string.UTF8StringUtil;

@MissingNullInOutFunction
public class FromBaseDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;

    public static final IFunctionDescriptorFactory FACTORY = FromBaseDescriptor::new;

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(final IEvaluatorContext ctx) throws HyracksDataException {
                return new IScalarEvaluator() {

                    private final ArrayBackedValueStorage storage = new ArrayBackedValueStorage();
                    private final StringBuilder strBuilder = new StringBuilder();
                    private final IScalarEvaluator evalString = args[0].createScalarEvaluator(ctx);
                    private final IScalarEvaluator evalBase = args[1].createScalarEvaluator(ctx);
                    private final IPointable argString = new VoidPointable();
                    private final IPointable argNumber = new VoidPointable();
                    private final AMutableInt32 aInt32 = new AMutableInt32(0);
                    private final AMutableInt64 aInt64 = new AMutableInt64(0);
                    @SuppressWarnings("unchecked")
                    private final ISerializerDeserializer<AInt64> int64Serde =
                            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT64);

                    @Override
                    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
                        evalString.evaluate(tuple, argString);
                        evalBase.evaluate(tuple, argNumber);

                        if (PointableHelper.checkAndSetMissingOrNull(result, argString, argNumber)) {
                            return;
                        }
                        byte[] bytes = argNumber.getByteArray();
                        int offset = argNumber.getStartOffset();
                        if (!ArgumentUtils.setInteger(ctx, sourceLoc, getIdentifier(), 1, bytes, offset, aInt32)) {
                            PointableHelper.setNull(result);
                            return;
                        }
                        int base = aInt32.getIntegerValue();
                        if (base < 2 || base > 36) {
                            PointableHelper.setNull(result);
                            ExceptionUtil.warnValueOutOfRange(ctx, sourceLoc, getIdentifier(), 1, 2, 36, base);
                            return;
                        }

                        bytes = argString.getByteArray();
                        offset = argString.getStartOffset();
                        if (bytes[offset] != ATypeTag.SERIALIZED_STRING_TYPE_TAG) {
                            PointableHelper.setNull(result);
                            ExceptionUtil.warnTypeMismatch(ctx, sourceLoc, getIdentifier(), bytes[offset], 0,
                                    ATypeTag.STRING);
                            return;
                        }
                        strBuilder.setLength(0);
                        UTF8StringUtil.toString(strBuilder, bytes, offset + 1);

                        try {
                            aInt64.setValue(Long.parseLong(strBuilder, 0, strBuilder.length(), base));
                        } catch (NumberFormatException e) {
                            PointableHelper.setNull(result);
                            ExceptionUtil.warnFunctionEvalFailed(ctx, sourceLoc, getIdentifier(), e.getMessage());
                            return;
                        }
                        storage.reset();
                        int64Serde.serialize(aInt64, storage.getDataOutput());
                        result.set(storage);
                    }
                };
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.FROM_BASE;
    }

}
