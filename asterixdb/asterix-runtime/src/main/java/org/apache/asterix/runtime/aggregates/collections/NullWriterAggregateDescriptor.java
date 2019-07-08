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
package org.apache.asterix.runtime.aggregates.collections;

import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.runtime.aggregates.base.AbstractAggregateFunctionDynamicDescriptor;
import org.apache.asterix.runtime.aggregates.std.AbstractAggregateFunction;
import org.apache.asterix.runtime.evaluators.functions.PointableHelper;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IAggregateEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IAggregateEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class NullWriterAggregateDescriptor extends AbstractAggregateFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = NullWriterAggregateDescriptor::new;

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.NULL_WRITER;
    }

    @Override
    public IAggregateEvaluatorFactory createAggregateEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IAggregateEvaluatorFactory() {

            private static final long serialVersionUID = 1L;

            @Override
            public IAggregateEvaluator createAggregateEvaluator(IEvaluatorContext ctx) {
                return new AbstractAggregateFunction(sourceLoc) {

                    @Override
                    public void init() {
                        // do nothing
                    }

                    @Override
                    public void step(IFrameTupleReference tuple) {
                        // do nothing
                    }

                    @Override
                    public void finish(IPointable result) {
                        PointableHelper.setNull(result);
                    }

                    @Override
                    public void finishPartial(IPointable result) {
                        finish(result);
                    }
                };
            }
        };
    }
}
