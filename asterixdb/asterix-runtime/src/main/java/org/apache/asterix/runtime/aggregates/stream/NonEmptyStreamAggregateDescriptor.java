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
package org.apache.asterix.runtime.aggregates.stream;

import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.runtime.aggregates.base.AbstractAggregateFunctionDynamicDescriptor;
import org.apache.asterix.runtime.aggregates.std.AbstractAggregateFunction;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IAggregateEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IAggregateEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class NonEmptyStreamAggregateDescriptor extends AbstractAggregateFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new NonEmptyStreamAggregateDescriptor();
        }
    };

    @Override
    public IAggregateEvaluatorFactory createAggregateEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        return new IAggregateEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IAggregateEvaluator createAggregateEvaluator(final IEvaluatorContext ctx)
                    throws HyracksDataException {

                return new AbstractAggregateFunction(sourceLoc) {

                    private ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
                    @SuppressWarnings("rawtypes")
                    private ISerializerDeserializer serde =
                            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ABOOLEAN);

                    boolean res = false;

                    @Override
                    public void init() throws HyracksDataException {
                        res = false;
                    }

                    @Override
                    public void step(IFrameTupleReference tuple) throws HyracksDataException {
                        res = true;
                    }

                    @SuppressWarnings("unchecked")
                    @Override
                    public void finish(IPointable result) throws HyracksDataException {
                        resultStorage.reset();
                        ABoolean b = res ? ABoolean.TRUE : ABoolean.FALSE;
                        serde.serialize(b, resultStorage.getDataOutput());
                        result.set(resultStorage);
                    }

                    @Override
                    public void finishPartial(IPointable result) throws HyracksDataException {
                        finish(result);
                    }
                };
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.NON_EMPTY_STREAM;
    }

}
