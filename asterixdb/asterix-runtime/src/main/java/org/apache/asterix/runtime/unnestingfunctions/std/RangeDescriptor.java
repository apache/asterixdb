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
package org.apache.asterix.runtime.unnestingfunctions.std;

import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.AMutableInt64;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.asterix.runtime.unnestingfunctions.base.AbstractUnnestingFunctionDynamicDescriptor;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.base.IUnnestingEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IUnnestingEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class RangeDescriptor extends AbstractUnnestingFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new RangeDescriptor();
        }
    };

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.RANGE;
    }

    @Override
    public IUnnestingEvaluatorFactory createUnnestingEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IUnnestingEvaluatorFactory() {

            private static final long serialVersionUID = 1L;

            @Override
            public IUnnestingEvaluator createUnnestingEvaluator(final IEvaluatorContext ctx)
                    throws HyracksDataException {
                return new IUnnestingEvaluator() {
                    private ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
                    @SuppressWarnings("rawtypes")
                    private ISerializerDeserializer serde =
                            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT64);
                    private IPointable inputVal = new VoidPointable();
                    private IScalarEvaluator eval0 = args[0].createScalarEvaluator(ctx);
                    private IScalarEvaluator eval1 = args[1].createScalarEvaluator(ctx);
                    private AMutableInt64 aInt64 = new AMutableInt64(0);
                    private long current;
                    private long max;

                    @Override
                    public void init(IFrameTupleReference tuple) throws HyracksDataException {
                        eval0.evaluate(tuple, inputVal);
                        current = ATypeHierarchy.getLongValue(getIdentifier().getName(), 0, inputVal.getByteArray(),
                                inputVal.getStartOffset());
                        eval1.evaluate(tuple, inputVal);
                        max = ATypeHierarchy.getLongValue(getIdentifier().getName(), 1, inputVal.getByteArray(),
                                inputVal.getStartOffset());
                    }

                    @SuppressWarnings("unchecked")
                    @Override
                    public boolean step(IPointable result) throws HyracksDataException {
                        if (current > max) {
                            return false;
                        }
                        aInt64.setValue(current);
                        resultStorage.reset();
                        serde.serialize(aInt64, resultStorage.getDataOutput());
                        result.set(resultStorage);
                        current++;
                        return true;
                    }

                };
            }
        };
    }

}
