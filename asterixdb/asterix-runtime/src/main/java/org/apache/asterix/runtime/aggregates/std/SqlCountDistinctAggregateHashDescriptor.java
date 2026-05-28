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
package org.apache.asterix.runtime.aggregates.std;

import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.aggregates.base.AbstractAggregateFunctionDynamicDescriptor;
import org.apache.asterix.runtime.functions.FunctionTypeInferers;
import org.apache.asterix.runtime.utils.DescriptorFactoryUtil;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IAggregateEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IAggregateEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IEvaluatorContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * Descriptor for HASH_DISTINCT_COUNT, the hash-based distinct counter.
 * See {@link SqlCountDistinctAggregateHashFunction}.
 */
public class SqlCountDistinctAggregateHashDescriptor extends AbstractAggregateFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    // SET_ARGUMENT_TYPE_AND_AGGREGATE_HASH_MEMORY passes both the argument's actual type (states[0]) and
    // the frame budget (states[1], from compiler.aggregate.distinct.hash.memory). The type specializes the
    // hash/comparator; the budget bounds memory by letting the evaluator spill to disk once exceeded.
    public static final IFunctionDescriptorFactory FACTORY =
            DescriptorFactoryUtil.createFactory(SqlCountDistinctAggregateHashDescriptor::new,
                    FunctionTypeInferers.SET_ARGUMENT_TYPE_AND_AGGREGATE_HASH_MEMORY);

    private IAType aggFieldType;
    private int numFrames;

    @Override
    public void setImmutableStates(Object... states) {
        aggFieldType = (IAType) states[0];
        numFrames = (int) states[1];
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.SQL_COUNT_DISTINCT_HASH;
    }

    @Override
    public IAggregateEvaluatorFactory createAggregateEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IAggregateEvaluatorFactory() {

            private static final long serialVersionUID = 1L;

            @Override
            public IAggregateEvaluator createAggregateEvaluator(IEvaluatorContext ctx) throws HyracksDataException {
                return new SqlCountDistinctAggregateHashFunction(args, ctx, sourceLoc, aggFieldType, numFrames);
            }
        };
    }
}
