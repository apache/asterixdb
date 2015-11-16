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
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluator;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IDataOutputProvider;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.data.std.util.GrowableArray;
import org.apache.hyracks.data.std.util.UTF8StringBuilder;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class Substring2Descriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;

    // allowed input types
    private static final byte SER_STRING_TYPE_TAG = ATypeTag.STRING.serialize();

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new Substring2Descriptor();
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
                    private final byte stt = ATypeTag.STRING.serialize();

                    private final GrowableArray array = new GrowableArray();
                    private final UTF8StringBuilder builder = new UTF8StringBuilder();
                    private final UTF8StringPointable string = new UTF8StringPointable();

                    @Override
                    public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
                        argOut.reset();
                        evalStart.evaluate(tuple);
                        int start = 0;

                        try {
                            start = ATypeHierarchy.getIntegerValue(argOut.getByteArray(), 0) - 1;
                        } catch (HyracksDataException e1) {
                            throw new AlgebricksException(e1);
                        }
                        argOut.reset();
                        evalString.evaluate(tuple);

                        byte[] bytes = argOut.getByteArray();
                        if (bytes[0] != SER_STRING_TYPE_TAG) {
                            throw new AlgebricksException(AsterixBuiltinFunctions.SUBSTRING2.getName()
                                    + ": expects type STRING for the first argument but got "
                                    + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(argOut.getByteArray()[0]));
                        }
                        string.set(bytes, 1, bytes.length);
                        array.reset();
                        try {
                            UTF8StringPointable.substr(string, start, Integer.MAX_VALUE, builder, array);
                        } catch (StringIndexOutOfBoundsException e) {
                            throw new AlgebricksException(AsterixBuiltinFunctions.SUBSTRING.getName() + ": start="
                                    + start + "\tgoing past the input length.");
                        } catch (IOException e) {
                            throw new AlgebricksException(e);
                        }

                        try {
                            out.writeByte(stt);
                            out.write(array.getByteArray(), 0, array.getLength());
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
        return AsterixBuiltinFunctions.SUBSTRING2;
    }

}
