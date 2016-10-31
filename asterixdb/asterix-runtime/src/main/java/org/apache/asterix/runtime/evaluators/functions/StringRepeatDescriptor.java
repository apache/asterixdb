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

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.exceptions.TypeMismatchException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import org.apache.hyracks.util.string.UTF8StringUtil;

public class StringRepeatDescriptor extends AbstractScalarFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new StringRepeatDescriptor();
        }
    };

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(final IHyracksTaskContext ctx) throws HyracksDataException {
                return new IScalarEvaluator() {
                    // Argument evaluators.
                    private IScalarEvaluator evalString = args[0].createScalarEvaluator(ctx);
                    private IScalarEvaluator evalStart = args[1].createScalarEvaluator(ctx);

                    // Argument pointers.
                    private IPointable argString = new VoidPointable();
                    private IPointable argNumber = new VoidPointable();

                    // For outputting the result.
                    private ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
                    private DataOutput out = resultStorage.getDataOutput();
                    private byte[] tempLengthArray = new byte[5];

                    @Override
                    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
                        resultStorage.reset();

                        // Calls argument evaluators.
                        evalString.evaluate(tuple, argString);
                        evalStart.evaluate(tuple, argNumber);

                        // Gets the repeating times.
                        byte[] bytes = argNumber.getByteArray();
                        int offset = argNumber.getStartOffset();
                        int repeatingTimes = ATypeHierarchy.getIntegerValue(getIdentifier().getName(), 1, bytes,
                                offset);
                        // Checks repeatingTimes. It should be a non-negative value.
                        if (repeatingTimes < 0) {
                            throw new RuntimeDataException(ErrorCode.ERROR_NEGATIVE_VALUE, getIdentifier(), 1,
                                    repeatingTimes);
                        }

                        // Gets the input string.
                        bytes = argString.getByteArray();
                        offset = argString.getStartOffset();
                        // Checks the type of the string argument.
                        if (bytes[offset] != ATypeTag.SERIALIZED_STRING_TYPE_TAG) {
                            throw new TypeMismatchException(getIdentifier(), 0, bytes[offset],
                                    ATypeTag.SERIALIZED_STRING_TYPE_TAG);
                        }

                        // Calculates the result string length.
                        int inputLen = UTF8StringUtil.getUTFLength(bytes, offset + 1);
                        int resultLen = Math.multiplyExact(inputLen, repeatingTimes); // Can throw overflow exception.
                        int cbytes = UTF8StringUtil.encodeUTF8Length(resultLen, tempLengthArray, 0);

                        // Writes the output string.
                        int inputStringStart = offset + 1 + UTF8StringUtil.getNumBytesToStoreLength(inputLen);
                        try {
                            out.writeByte(ATypeTag.SERIALIZED_STRING_TYPE_TAG);
                            out.write(tempLengthArray, 0, cbytes);
                            for (int numRepeats = 0; numRepeats < repeatingTimes; ++numRepeats) {
                                out.write(bytes, inputStringStart, inputLen);
                            }
                        } catch (IOException e) {
                            throw new HyracksDataException(e);
                        }
                        result.set(resultStorage);
                    }
                };
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return AsterixBuiltinFunctions.STRING_REPEAT;
    }

}
