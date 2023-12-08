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

package org.apache.asterix.runtime.aggregates.serializable.std;

import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.runtime.aggregates.base.AbstractSerializableAggregateFunctionDynamicDescriptor;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.base.ISerializedAggregateEvaluator;
import org.apache.hyracks.algebricks.runtime.base.ISerializedAggregateEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class SerializableGlobalSqlSchemaAggregateDescriptor
        extends AbstractSerializableAggregateFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = SerializableGlobalSqlSchemaAggregateDescriptor::new;

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.SERIAL_GLOBAL_SQL_SCHEMA;
    }

    @Override
    public ISerializedAggregateEvaluatorFactory createSerializableAggregateEvaluatorFactory(
            final IScalarEvaluatorFactory[] args) {
        return new ISerializedAggregateEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public ISerializedAggregateEvaluator createAggregateEvaluator(IEvaluatorContext ctx)
                    throws HyracksDataException {
                return new SerializableGlobalSqlAvgAggregateFunction(args, ctx, sourceLoc);
            }
        };
    }

}
