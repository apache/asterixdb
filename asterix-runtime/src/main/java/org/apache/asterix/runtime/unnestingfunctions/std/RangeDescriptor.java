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

import org.apache.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import org.apache.asterix.om.base.AMutableInt64;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.asterix.runtime.unnestingfunctions.base.AbstractUnnestingFunctionDynamicDescriptor;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.base.IUnnestingEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IUnnestingEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
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
        return AsterixBuiltinFunctions.RANGE;
    }

    @Override
    public IUnnestingEvaluatorFactory createUnnestingEvaluatorFactory(final IScalarEvaluatorFactory[] args)
            throws AlgebricksException {
        return new IUnnestingEvaluatorFactory() {

            private static final long serialVersionUID = 1L;

            @Override
            public IUnnestingEvaluator createUnnestingEvaluator(final IHyracksTaskContext ctx)
                    throws AlgebricksException {
                return new IUnnestingEvaluator() {

                    private ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
                    @SuppressWarnings("rawtypes")
                    private ISerializerDeserializer serde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.AINT64);
                    private IPointable inputVal = new VoidPointable();
                    private IScalarEvaluator eval0 = args[0].createScalarEvaluator(ctx);
                    private IScalarEvaluator eval1 = args[1].createScalarEvaluator(ctx);
                    private AMutableInt64 aInt64 = new AMutableInt64(0);
                    private long current;
                    private long max;

                    @Override
                    public void init(IFrameTupleReference tuple) throws AlgebricksException {
                        eval0.evaluate(tuple, inputVal);
                        try {
                            current = ATypeHierarchy.getLongValue(inputVal.getByteArray(), inputVal.getStartOffset());
                        } catch (HyracksDataException e) {
                            throw new AlgebricksException(e);
                        }
                        eval1.evaluate(tuple, inputVal);
                        try {
                            max = ATypeHierarchy.getLongValue(inputVal.getByteArray(), inputVal.getStartOffset());
                        } catch (HyracksDataException e) {
                            throw new AlgebricksException(e);
                        }
                    }

                    @SuppressWarnings("unchecked")
                    @Override
                    public boolean step(IPointable result) throws AlgebricksException {
                        if (current > max) {
                            return false;
                        }
                        aInt64.setValue(current);
                        try {
                            resultStorage.reset();
                            serde.serialize(aInt64, resultStorage.getDataOutput());
                            result.set(resultStorage);
                        } catch (HyracksDataException e) {
                            throw new AlgebricksException(e);
                        }
                        current++;
                        return true;
                    }

                };
            }
        };
    }

}
