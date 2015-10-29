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
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluator;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import org.apache.hyracks.data.std.api.IDataOutputProvider;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.data.std.util.GrowableArray;
import org.apache.hyracks.data.std.util.UTF8StringBuilder;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class SubstringAfterDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;

    // allowed input types
    private static final byte SER_STRING_TYPE_TAG = ATypeTag.STRING.serialize();
    private static final byte SER_NULL_TYPE_TAG = ATypeTag.NULL.serialize();

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new SubstringAfterDescriptor();
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
                    private ArrayBackedValueStorage array0 = new ArrayBackedValueStorage();
                    private ArrayBackedValueStorage array1 = new ArrayBackedValueStorage();
                    private ICopyEvaluator evalString = args[0].createEvaluator(array0);
                    private ICopyEvaluator evalPattern = args[1].createEvaluator(array1);
                    private final byte stt = ATypeTag.STRING.serialize();

                    private final GrowableArray array = new GrowableArray();
                    private final UTF8StringBuilder builder = new UTF8StringBuilder();
                    private final UTF8StringPointable stringPtr = new UTF8StringPointable();
                    private final UTF8StringPointable patternPtr = new UTF8StringPointable();

                    @Override
                    public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
                        array0.reset();
                        evalString.evaluate(tuple);
                        byte[] src = array0.getByteArray();

                        array1.reset();
                        evalPattern.evaluate(tuple);
                        byte[] pattern = array1.getByteArray();

                        if ((src[0] != SER_STRING_TYPE_TAG && src[0] != SER_NULL_TYPE_TAG)
                                || (pattern[0] != SER_STRING_TYPE_TAG && pattern[0] != SER_NULL_TYPE_TAG)) {
                            throw new AlgebricksException(AsterixBuiltinFunctions.SUBSTRING_AFTER.getName()
                                    + ": expects input type (STRING/NULL, STRING/NULL) but got ("
                                    + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(src[0]) + ", "
                                    + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(pattern[0]) + ").");
                        }

                        stringPtr.set(src, 1, src.length);
                        patternPtr.set(pattern, 1, pattern.length);
                        array.reset();
                        try {
                            UTF8StringPointable.substrAfter(stringPtr, patternPtr, builder, array);
                            out.writeByte(stt);
                            out.write(array.getByteArray(), 0, array.getLength());
                        } catch (IOException e) {
                            throw new AlgebricksException(e);
                        }

                    }
                }

                        ;
            }
        }

                ;
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return AsterixBuiltinFunctions.SUBSTRING_AFTER;
    }

}
