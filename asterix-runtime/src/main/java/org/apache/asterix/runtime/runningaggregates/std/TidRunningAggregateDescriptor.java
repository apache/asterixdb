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
package org.apache.asterix.runtime.runningaggregates.std;

import java.io.DataOutput;

import org.apache.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.AMutableInt64;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.runtime.runningaggregates.base.AbstractRunningAggregateFunctionDynamicDescriptor;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.base.ICopyRunningAggregateFunction;
import org.apache.hyracks.algebricks.runtime.base.ICopyRunningAggregateFunctionFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IDataOutputProvider;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class TidRunningAggregateDescriptor extends AbstractRunningAggregateFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new TidRunningAggregateDescriptor();
        }
    };

    @Override
    public ICopyRunningAggregateFunctionFactory createRunningAggregateFunctionFactory(ICopyEvaluatorFactory[] args)
            throws AlgebricksException {

        return new ICopyRunningAggregateFunctionFactory() {

            private static final long serialVersionUID = 1L;

            @SuppressWarnings("unchecked")
            @Override
            public ICopyRunningAggregateFunction createRunningAggregateFunction(IDataOutputProvider provider)
                    throws AlgebricksException {

                final DataOutput out = provider.getDataOutput();

                return new ICopyRunningAggregateFunction() {

                    int cnt;
                    ISerializerDeserializer<AInt64> serde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.AINT64);
                    AMutableInt64 m = new AMutableInt64(0);

                    @Override
                    public void step(IFrameTupleReference tuple) throws AlgebricksException {
                        try {
                            m.setValue(cnt);
                            serde.serialize(m, out);
                        } catch (HyracksDataException e) {
                            throw new AlgebricksException(e);
                        }

                        ++cnt;
                    }

                    @Override
                    public void init() throws AlgebricksException {
                        cnt = 1;
                    }
                };
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return AsterixBuiltinFunctions.TID;
    }

}
