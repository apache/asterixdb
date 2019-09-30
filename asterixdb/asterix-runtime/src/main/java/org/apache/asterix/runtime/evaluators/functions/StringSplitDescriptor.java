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

import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.common.annotations.MissingNullInOutFunction;
import org.apache.asterix.om.exceptions.ExceptionUtil;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
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
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import org.apache.hyracks.util.string.UTF8StringUtil;

@MissingNullInOutFunction
public class StringSplitDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new StringSplitDescriptor();
        }
    };

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(final IEvaluatorContext ctx) throws HyracksDataException {
                return new IScalarEvaluator() {
                    // Argument evaluators.
                    private final IScalarEvaluator stringEval = args[0].createScalarEvaluator(ctx);
                    private final IScalarEvaluator patternEval = args[1].createScalarEvaluator(ctx);

                    // Argument pointers.
                    private final IPointable argString = new VoidPointable();
                    private final IPointable argPattern = new VoidPointable();
                    private final UTF8StringPointable argStrPtr = new UTF8StringPointable();
                    private final UTF8StringPointable argPatternPtr = new UTF8StringPointable();

                    // For an output string item.
                    private final ArrayBackedValueStorage itemStorge = new ArrayBackedValueStorage();
                    private final DataOutput itemOut = itemStorge.getDataOutput();
                    private final byte[] tempLengthArray = new byte[5];

                    // For the output list of strings.
                    private final AOrderedListType intListType = new AOrderedListType(BuiltinType.ASTRING, null);
                    private final OrderedListBuilder listBuilder = new OrderedListBuilder();
                    private final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
                    private final DataOutput out = resultStorage.getDataOutput();

                    @Override
                    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
                        try {
                            resultStorage.reset();
                            // Calls argument evaluators.
                            stringEval.evaluate(tuple, argString);
                            patternEval.evaluate(tuple, argPattern);

                            if (PointableHelper.checkAndSetMissingOrNull(result, argString, argPattern)) {
                                return;
                            }

                            // Gets the bytes of the source string.
                            byte[] srcString = argString.getByteArray();
                            int srcOffset = argString.getStartOffset();
                            int srcLen = argString.getLength();
                            // Type check for the first argument.
                            if (srcString[srcOffset] != ATypeTag.SERIALIZED_STRING_TYPE_TAG) {
                                PointableHelper.setNull(result);
                                ExceptionUtil.warnTypeMismatch(ctx, sourceLoc, getIdentifier(), srcString[srcOffset], 0,
                                        ATypeTag.STRING);
                                return;
                            }

                            // Gets the bytes of the pattern string.
                            byte[] patternString = argPattern.getByteArray();
                            int patternOffset = argPattern.getStartOffset();
                            int patternLen = argPattern.getLength();
                            // Type check for the second argument.
                            if (patternString[patternOffset] != ATypeTag.SERIALIZED_STRING_TYPE_TAG) {
                                PointableHelper.setNull(result);
                                ExceptionUtil.warnTypeMismatch(ctx, sourceLoc, getIdentifier(),
                                        patternString[patternOffset], 1, ATypeTag.STRING);
                                return;
                            }

                            // Sets the UTF8 String pointables.
                            argStrPtr.set(srcString, srcOffset + 1, srcLen - 1);
                            argPatternPtr.set(patternString, patternOffset + 1, patternLen - 1);

                            // Gets the string length of the source string.
                            int inputStringLen = UTF8StringUtil.getUTFLength(srcString, srcOffset + 1);
                            int inputStringStart =
                                    srcOffset + 1 + UTF8StringUtil.getNumBytesToStoreLength(inputStringLen);
                            // Gets the string length of the pattern string.
                            int inputPatternLen = UTF8StringUtil.getUTFLength(patternString, patternOffset + 1);
                            // Handles the case that the pattern is "".
                            boolean emptyStringPattern = inputPatternLen == 0;

                            // Builds a list of strings.
                            listBuilder.reset(intListType);
                            int itemStrStart = 0;
                            int nextMatchStart;
                            while (itemStrStart < inputStringLen && (nextMatchStart =
                                    UTF8StringPointable.find(argStrPtr, argPatternPtr, false, itemStrStart)) >= 0) {
                                // Adds an item string.
                                addItemString(srcString, inputStringStart, itemStrStart,
                                        emptyStringPattern ? nextMatchStart + 1 : nextMatchStart);
                                itemStrStart = nextMatchStart + (emptyStringPattern ? 1 : inputPatternLen);
                            }
                            if (!emptyStringPattern) {
                                addItemString(srcString, inputStringStart, itemStrStart, inputStringLen);
                            }
                            listBuilder.write(out, true);
                            result.set(resultStorage);
                        } catch (IOException e1) {
                            throw HyracksDataException.create(e1);
                        }
                    }

                    private void addItemString(byte[] srcString, int inputStringStart, int itemStartOffset,
                            int nextMatchStart) throws IOException {
                        int itemLen = nextMatchStart - itemStartOffset;
                        int cbytes = UTF8StringUtil.encodeUTF8Length(itemLen, tempLengthArray, 0);
                        itemStorge.reset();
                        itemOut.writeByte(ATypeTag.SERIALIZED_STRING_TYPE_TAG);
                        itemOut.write(tempLengthArray, 0, cbytes);
                        if (itemLen > 0) {
                            itemOut.write(srcString, inputStringStart + itemStartOffset, itemLen);
                        }
                        listBuilder.addItem(itemStorge);
                    }
                };
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.STRING_SPLIT;
    }
}
