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

import java.io.DataOutput;
import java.io.IOException;

import org.apache.asterix.common.annotations.MissingNullInOutFunction;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.functions.IFunctionTypeInferer;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.asterix.runtime.exceptions.TypeMismatchException;
import org.apache.asterix.runtime.functions.FunctionTypeInferers;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.data.std.util.GrowableArray;
import org.apache.hyracks.data.std.util.UTF8StringBuilder;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

@MissingNullInOutFunction
public class SubstringDescriptor extends AbstractStringOffsetConfigurableDescriptor {

    private static final long serialVersionUID = 1L;

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new SubstringDescriptor();
        }

        @Override
        public IFunctionTypeInferer createFunctionTypeInferer() {
            return FunctionTypeInferers.SET_STRING_OFFSET;
        }
    };

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            private final int baseOffset = stringOffset;

            @Override
            public IScalarEvaluator createScalarEvaluator(final IEvaluatorContext ctx) throws HyracksDataException {
                return new IScalarEvaluator() {

                    private final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
                    private final DataOutput out = resultStorage.getDataOutput();
                    private IPointable argString = new VoidPointable();
                    private IPointable argStart = new VoidPointable();
                    private IPointable argLen = new VoidPointable();
                    private final IScalarEvaluator evalString = args[0].createScalarEvaluator(ctx);
                    private final IScalarEvaluator evalStart = args[1].createScalarEvaluator(ctx);
                    private final IScalarEvaluator evalLen = args[2].createScalarEvaluator(ctx);

                    private final GrowableArray array = new GrowableArray();
                    private final UTF8StringBuilder builder = new UTF8StringBuilder();
                    private final UTF8StringPointable string = new UTF8StringPointable();

                    @Override
                    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
                        resultStorage.reset();
                        evalString.evaluate(tuple, argString);
                        evalStart.evaluate(tuple, argStart);
                        evalLen.evaluate(tuple, argLen);

                        if (PointableHelper.checkAndSetMissingOrNull(result, argString, argStart, argLen)) {
                            return;
                        }

                        byte[] bytes = argStart.getByteArray();
                        int offset = argStart.getStartOffset();
                        int start = ATypeHierarchy.getIntegerValue(getIdentifier().getName(), 0, bytes, offset);

                        bytes = argLen.getByteArray();
                        offset = argLen.getStartOffset();
                        int len = ATypeHierarchy.getIntegerValue(getIdentifier().getName(), 1, bytes, offset);

                        bytes = argString.getByteArray();
                        offset = argString.getStartOffset();
                        int length = argString.getLength();
                        if (bytes[offset] != ATypeTag.SERIALIZED_STRING_TYPE_TAG) {
                            throw new TypeMismatchException(sourceLoc, getIdentifier(), 0, bytes[offset],
                                    ATypeTag.SERIALIZED_STRING_TYPE_TAG);
                        }
                        string.set(bytes, offset + 1, length - 1);
                        array.reset();
                        try {
                            int actualStart = start >= 0 ? start - baseOffset : string.getStringLength() + start;
                            boolean success = UTF8StringPointable.substr(string, actualStart, len, builder, array);
                            if (success) {
                                out.writeByte(ATypeTag.SERIALIZED_STRING_TYPE_TAG);
                                out.write(array.getByteArray(), 0, array.getLength());
                                result.set(resultStorage);
                            } else {
                                PointableHelper.setNull(result);
                            }
                        } catch (IOException e) {
                            throw HyracksDataException.create(e);
                        }
                    }
                };
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.SUBSTRING;
    }

}
