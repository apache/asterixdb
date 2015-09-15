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
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluator;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import org.apache.hyracks.data.std.api.IDataOutputProvider;
import org.apache.hyracks.data.std.primitive.DoublePointable;
import org.apache.hyracks.data.std.primitive.FloatPointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.data.std.primitive.ShortPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class SubstringDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new SubstringDescriptor();
        }
    };

    @Override
    public ICopyEvaluatorFactory createEvaluatorFactory(final ICopyEvaluatorFactory[] args) throws AlgebricksException {
        return new ICopyEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public ICopyEvaluator createEvaluator(final IDataOutputProvider output) throws AlgebricksException {
                return new ICopyEvaluator() {

                    private DataOutput out = output.getDataOutput();
                    private ArrayBackedValueStorage argOut = new ArrayBackedValueStorage();
                    private ICopyEvaluator evalString = args[0].createEvaluator(argOut);
                    private ICopyEvaluator evalStart = args[1].createEvaluator(argOut);
                    private ICopyEvaluator evalLen = args[2].createEvaluator(argOut);
                    private final byte stt = ATypeTag.STRING.serialize();

                    @Override
                    public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
                        argOut.reset();
                        evalStart.evaluate(tuple);
                        int start = 0;

                        ATypeTag argOutTypeTag = ATypeTag.VALUE_TYPE_MAPPING[argOut.getByteArray()[0]];

                        switch (argOutTypeTag) {
                            case INT64:
                                start = (int) LongPointable.getLong(argOut.getByteArray(), 1) - 1;
                                break;
                            case INT32:
                                start = IntegerPointable.getInteger(argOut.getByteArray(), 1) - 1;
                                break;
                            case INT8:
                                start = argOut.getByteArray()[1] - 1;
                                break;
                            case INT16:
                                start = (int) ShortPointable.getShort(argOut.getByteArray(), 1) - 1;
                                break;
                            case FLOAT:
                                start = (int) FloatPointable.getFloat(argOut.getByteArray(), 1) - 1;
                                break;
                            case DOUBLE:
                                start = (int) DoublePointable.getDouble(argOut.getByteArray(), 1) - 1;
                                break;
                            default:
                                throw new AlgebricksException(AsterixBuiltinFunctions.SUBSTRING.getName()
                                        + ": expects type INT8/16/32/64/FLOAT/DOUBLE for the second argument but got "
                                        + argOutTypeTag);
                        }

                        argOut.reset();
                        evalLen.evaluate(tuple);
                        int len = 0;

                        argOutTypeTag = ATypeTag.VALUE_TYPE_MAPPING[argOut.getByteArray()[0]];

                        switch (argOutTypeTag) {
                            case INT64:
                                len = (int) LongPointable.getLong(argOut.getByteArray(), 1);
                                break;
                            case INT32:
                                len = IntegerPointable.getInteger(argOut.getByteArray(), 1);
                                break;
                            case INT8:
                                len = argOut.getByteArray()[1];
                                break;
                            case INT16:
                                len = (int) ShortPointable.getShort(argOut.getByteArray(), 1);
                                break;
                            case FLOAT:
                                len = (int) FloatPointable.getFloat(argOut.getByteArray(), 1);
                                break;
                            case DOUBLE:
                                len = (int) DoublePointable.getDouble(argOut.getByteArray(), 1);
                                break;
                            default:
                                throw new AlgebricksException(AsterixBuiltinFunctions.SUBSTRING.getName()
                                        + ": expects type INT8/16/32/64/FLOAT/DOUBLE for the third argument but got "
                                        + argOutTypeTag);
                        }

                        argOut.reset();
                        evalString.evaluate(tuple);

                        byte[] bytes = argOut.getByteArray();
                        argOutTypeTag = ATypeTag.VALUE_TYPE_MAPPING[bytes[0]];

                        if (argOutTypeTag != ATypeTag.STRING) {
                            throw new AlgebricksException(AsterixBuiltinFunctions.SUBSTRING.getName()
                                    + ": expects type STRING for the first argument but got " + argOutTypeTag);
                        }
                        int utflen = UTF8StringPointable.getUTFLength(bytes, 1);
                        int sStart = 3;
                        int c = 0;
                        int idxPos1 = 0;
                        // skip to start
                        while (idxPos1 < start && c < utflen) {
                            c += UTF8StringPointable.charSize(bytes, sStart + c);
                            ++idxPos1;
                        }
                        int startSubstr = c;
                        int idxPos2 = 0;
                        while (idxPos2 < len && c < utflen) {
                            c += UTF8StringPointable.charSize(bytes, sStart + c);
                            ++idxPos2;
                        }

                        if (idxPos2 < len) {
                            throw new AlgebricksException(AsterixBuiltinFunctions.SUBSTRING.getName() + ": start="
                                    + start + "\tlen=" + len + "\tgoing past the input length=" + (idxPos1 + idxPos2)
                                    + ".");
                        }

                        int substrByteLen = c - startSubstr;
                        try {
                            out.writeByte(stt);
                            out.writeByte((byte) ((substrByteLen >>> 8) & 0xFF));
                            out.writeByte((byte) ((substrByteLen >>> 0) & 0xFF));
                            out.write(bytes, sStart + startSubstr, substrByteLen);

                        } catch (IOException e) {
                            throw new AlgebricksException(e);
                        }
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
