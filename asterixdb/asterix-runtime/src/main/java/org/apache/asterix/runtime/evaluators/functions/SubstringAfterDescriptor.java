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
import org.apache.asterix.om.exceptions.ExceptionUtil;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
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
public class SubstringAfterDescriptor extends AbstractScalarFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new SubstringAfterDescriptor();
        }
    };

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(final IEvaluatorContext ctx) throws HyracksDataException {
                return new IScalarEvaluator() {
                    private ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
                    private DataOutput out = resultStorage.getDataOutput();
                    private IPointable array0 = new VoidPointable();
                    private IPointable array1 = new VoidPointable();
                    private IScalarEvaluator evalString = args[0].createScalarEvaluator(ctx);
                    private IScalarEvaluator evalPattern = args[1].createScalarEvaluator(ctx);
                    private final GrowableArray array = new GrowableArray();
                    private final UTF8StringBuilder builder = new UTF8StringBuilder();
                    private final UTF8StringPointable stringPtr = new UTF8StringPointable();
                    private final UTF8StringPointable patternPtr = new UTF8StringPointable();

                    @Override
                    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
                        resultStorage.reset();
                        evalString.evaluate(tuple, array0);
                        evalPattern.evaluate(tuple, array1);

                        if (PointableHelper.checkAndSetMissingOrNull(result, array0, array1)) {
                            return;
                        }

                        byte[] src = array0.getByteArray();
                        int srcOffset = array0.getStartOffset();
                        int srcLen = array0.getLength();
                        byte[] pattern = array1.getByteArray();
                        int patternOffset = array1.getStartOffset();
                        int patternLen = array1.getLength();

                        if (src[srcOffset] != ATypeTag.SERIALIZED_STRING_TYPE_TAG) {
                            PointableHelper.setNull(result);
                            ExceptionUtil.warnTypeMismatch(ctx, sourceLoc, getIdentifier(), src[srcOffset], 0,
                                    ATypeTag.STRING);
                            return;
                        }
                        if (pattern[patternOffset] != ATypeTag.SERIALIZED_STRING_TYPE_TAG) {
                            PointableHelper.setNull(result);
                            ExceptionUtil.warnTypeMismatch(ctx, sourceLoc, getIdentifier(), pattern[patternOffset], 1,
                                    ATypeTag.STRING);
                            return;
                        }

                        try {
                            stringPtr.set(src, srcOffset + 1, srcLen - 1);
                            patternPtr.set(pattern, patternOffset + 1, patternLen - 1);
                            array.reset();

                            UTF8StringPointable.substrAfter(stringPtr, patternPtr, builder, array);
                            out.writeByte(ATypeTag.SERIALIZED_STRING_TYPE_TAG);
                            out.write(array.getByteArray(), 0, array.getLength());
                        } catch (IOException e) {
                            throw HyracksDataException.create(e);
                        }
                        result.set(resultStorage);
                    }
                }

                ;
            }
        }

        ;
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.SUBSTRING_AFTER;
    }
}
