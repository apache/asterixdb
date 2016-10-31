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
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.data.std.util.GrowableArray;
import org.apache.hyracks.data.std.util.UTF8StringBuilder;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class Substring2Descriptor extends AbstractScalarFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new Substring2Descriptor();
        }
    };

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(final IHyracksTaskContext ctx) throws HyracksDataException {
                return new IScalarEvaluator() {
                    private ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
                    private DataOutput out = resultStorage.getDataOutput();
                    private IPointable argString = new VoidPointable();
                    private IPointable argStart = new VoidPointable();
                    private IScalarEvaluator evalString = args[0].createScalarEvaluator(ctx);
                    private IScalarEvaluator evalStart = args[1].createScalarEvaluator(ctx);
                    private final GrowableArray array = new GrowableArray();
                    private final UTF8StringBuilder builder = new UTF8StringBuilder();
                    private final UTF8StringPointable string = new UTF8StringPointable();

                    @Override
                    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
                        resultStorage.reset();
                        evalString.evaluate(tuple, argString);
                        evalStart.evaluate(tuple, argStart);

                        byte[] bytes = argStart.getByteArray();
                        int offset = argStart.getStartOffset();
                        int start = ATypeHierarchy.getIntegerValue(getIdentifier().getName(), 1, bytes, offset) - 1;
                        bytes = argString.getByteArray();
                        offset = argString.getStartOffset();
                        int len = argString.getLength();
                        if (bytes[offset] != ATypeTag.SERIALIZED_STRING_TYPE_TAG) {
                            throw new TypeMismatchException(getIdentifier(), 0, bytes[offset],
                                    ATypeTag.SERIALIZED_STRING_TYPE_TAG);
                        }
                        string.set(bytes, offset + 1, len - 1);
                        array.reset();
                        try {
                            UTF8StringPointable.substr(string, start, Integer.MAX_VALUE, builder, array);
                        } catch (StringIndexOutOfBoundsException e) {
                            throw new RuntimeDataException(ErrorCode.ERROR_OUT_OF_BOUND, getIdentifier(), 1, start);
                        } catch (IOException e) {
                            throw new HyracksDataException(e);
                        }
                        try {
                            out.writeByte(ATypeTag.SERIALIZED_STRING_TYPE_TAG);
                            out.write(array.getByteArray(), 0, array.getLength());
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
        return AsterixBuiltinFunctions.SUBSTRING2;
    }

}
