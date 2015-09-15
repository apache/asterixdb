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

import java.io.DataOutput;

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
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluator;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.base.ICopyUnnestingFunction;
import org.apache.hyracks.algebricks.runtime.base.ICopyUnnestingFunctionFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IDataOutputProvider;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class RangeDescriptor extends AbstractUnnestingFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new RangeDescriptor();
        }
    };

    @Override
    public FunctionIdentifier getIdentifier() {
        return AsterixBuiltinFunctions.RANGE;
    }

    @Override
    public ICopyUnnestingFunctionFactory createUnnestingFunctionFactory(final ICopyEvaluatorFactory[] args)
            throws AlgebricksException {
        return new ICopyUnnestingFunctionFactory() {

            private static final long serialVersionUID = 1L;

            @Override
            public ICopyUnnestingFunction createUnnestingFunction(final IDataOutputProvider provider)
                    throws AlgebricksException {
                return new ICopyUnnestingFunction() {

                    private DataOutput out = provider.getDataOutput();
                    @SuppressWarnings("rawtypes")
                    private ISerializerDeserializer serde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.AINT64);
                    private ArrayBackedValueStorage inputVal = new ArrayBackedValueStorage();
                    private ICopyEvaluator eval0 = args[0].createEvaluator(inputVal);
                    private ICopyEvaluator eval1 = args[1].createEvaluator(inputVal);
                    private AMutableInt64 aInt64 = new AMutableInt64(0);
                    private long current;
                    private long max;

                    @Override
                    public void init(IFrameTupleReference tuple) throws AlgebricksException {
                        inputVal.reset();
                        eval0.evaluate(tuple);
                        try {
                            current = ATypeHierarchy.getLongValue(inputVal.getByteArray(), 0);
                        } catch (HyracksDataException e) {
                            throw new AlgebricksException(e);
                        }
                        inputVal.reset();
                        eval1.evaluate(tuple);
                        try {
                            max = ATypeHierarchy.getLongValue(inputVal.getByteArray(), 0);
                        } catch (HyracksDataException e) {
                            throw new AlgebricksException(e);
                        }
                    }

                    @SuppressWarnings("unchecked")
                    @Override
                    public boolean step() throws AlgebricksException {
                        if (current > max) {
                            return false;
                        }
                        aInt64.setValue(current);
                        try {
                            serde.serialize(aInt64, out);
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
