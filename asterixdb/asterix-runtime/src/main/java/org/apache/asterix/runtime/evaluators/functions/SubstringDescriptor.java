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

import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.DoublePointable;
import org.apache.hyracks.data.std.primitive.FloatPointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.data.std.primitive.ShortPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.data.std.util.GrowableArray;
import org.apache.hyracks.data.std.util.UTF8StringBuilder;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class SubstringDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new SubstringDescriptor();
        }
    };

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args)
            throws AlgebricksException {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(final IHyracksTaskContext ctx) throws AlgebricksException {
                return new IScalarEvaluator() {

                    private final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
                    private final DataOutput out = resultStorage.getDataOutput();
                    private final IPointable argPtr = new VoidPointable();
                    private final IScalarEvaluator evalString = args[0].createScalarEvaluator(ctx);
                    private final IScalarEvaluator evalStart = args[1].createScalarEvaluator(ctx);
                    private final IScalarEvaluator evalLen = args[2].createScalarEvaluator(ctx);

                    private final GrowableArray array = new GrowableArray();
                    private final UTF8StringBuilder builder = new UTF8StringBuilder();
                    private final UTF8StringPointable string = new UTF8StringPointable();

                    @Override
                    public void evaluate(IFrameTupleReference tuple, IPointable result) throws AlgebricksException {
                        resultStorage.reset();
                        evalStart.evaluate(tuple, argPtr);
                        byte[] bytes = argPtr.getByteArray();
                        int offset = argPtr.getStartOffset();
                        int start = 0;

                        ATypeTag argPtrTypeTag = ATypeTag.VALUE_TYPE_MAPPING[bytes[offset]];

                        switch (argPtrTypeTag) {
                            case INT64:
                                start = (int) LongPointable.getLong(bytes, offset + 1) - 1;
                                break;
                            case INT32:
                                start = IntegerPointable.getInteger(bytes, offset + 1) - 1;
                                break;
                            case INT8:
                                start = bytes[offset + 1] - 1;
                                break;
                            case INT16:
                                start = ShortPointable.getShort(bytes, offset + 1) - 1;
                                break;
                            case FLOAT:
                                start = (int) FloatPointable.getFloat(bytes, offset + 1) - 1;
                                break;
                            case DOUBLE:
                                start = (int) DoublePointable.getDouble(bytes, offset + 1) - 1;
                                break;
                            default:
                                throw new AlgebricksException(AsterixBuiltinFunctions.SUBSTRING.getName()
                                        + ": expects type INT8/16/32/64/FLOAT/DOUBLE for the second argument but got "
                                        + argPtrTypeTag);
                        }

                        evalLen.evaluate(tuple, argPtr);
                        bytes = argPtr.getByteArray();
                        offset = argPtr.getStartOffset();
                        int len = 0;

                        argPtrTypeTag = ATypeTag.VALUE_TYPE_MAPPING[bytes[offset]];

                        switch (argPtrTypeTag) {
                            case INT64:
                                len = (int) LongPointable.getLong(bytes, offset + 1);
                                break;
                            case INT32:
                                len = IntegerPointable.getInteger(bytes, offset + 1);
                                break;
                            case INT8:
                                len = bytes[offset + 1];
                                break;
                            case INT16:
                                len = ShortPointable.getShort(bytes, offset + 1);
                                break;
                            case FLOAT:
                                len = (int) FloatPointable.getFloat(bytes, offset + 1);
                                break;
                            case DOUBLE:
                                len = (int) DoublePointable.getDouble(bytes, offset + 1);
                                break;
                            default:
                                throw new AlgebricksException(AsterixBuiltinFunctions.SUBSTRING.getName()
                                        + ": expects type INT8/16/32/64/FLOAT/DOUBLE for the third argument but got "
                                        + argPtrTypeTag);
                        }

                        evalString.evaluate(tuple, argPtr);
                        bytes = argPtr.getByteArray();
                        offset = argPtr.getStartOffset();
                        int length = argPtr.getLength();
                        argPtrTypeTag = ATypeTag.VALUE_TYPE_MAPPING[bytes[offset]];

                        if (argPtrTypeTag != ATypeTag.STRING) {
                            throw new AlgebricksException(AsterixBuiltinFunctions.SUBSTRING.getName()
                                    + ": expects type STRING for the first argument but got " + argPtrTypeTag);
                        }

                        string.set(bytes, offset + 1, length - 1);
                        array.reset();
                        try {
                            UTF8StringPointable.substr(string, start, len, builder, array);
                        } catch (StringIndexOutOfBoundsException e) {
                            throw new AlgebricksException(AsterixBuiltinFunctions.SUBSTRING.getName() + ": start="
                                    + start + "\tgoing past the input length.");
                        } catch (IOException e) {
                            throw new AlgebricksException(e);
                        }

                        try {
                            out.writeByte(ATypeTag.SERIALIZED_STRING_TYPE_TAG);
                            out.write(array.getByteArray(), 0, array.getLength());
                        } catch (IOException e) {
                            throw new AlgebricksException(e);
                        }
                        result.set(resultStorage);
                    }
                };
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return AsterixBuiltinFunctions.SUBSTRING;
    }

}
