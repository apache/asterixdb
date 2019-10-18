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

import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.runtime.runningaggregates.base.AbstractRunningAggregateFunctionDynamicDescriptor;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IRunningAggregateEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IRunningAggregateEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 *  This internal window function returns {@code TRUE} in the following two cases:
 *  <ol>
 *      <li>the argument is not MISSING</li>
 *      <li>the argument is MISSING and it comes from the first tuple in the current window partition</li>
 *  </ol>
 *  In all other cases the function returns {@code FALSE}.
 *  <p>
 *  The underlying assumption is that tuples in each window partition are sorted on the function's argument in the
 *  descending order.
 */
public final class WinMarkFirstMissingRunningAggregateDescriptor
        extends AbstractRunningAggregateFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;

    public static final IFunctionDescriptorFactory FACTORY = WinMarkFirstMissingRunningAggregateDescriptor::new;

    @Override
    public IRunningAggregateEvaluatorFactory createRunningAggregateEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        return new IRunningAggregateEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IRunningAggregateEvaluator createRunningAggregateEvaluator(IEvaluatorContext ctx)
                    throws HyracksDataException {
                IScalarEvaluator[] evals = new IScalarEvaluator[args.length];
                for (int i = 0; i < args.length; i++) {
                    evals[i] = args[i].createScalarEvaluator(ctx);
                }
                return new WinMarkFirstMissingRunningAggregateEvaluator(evals);
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.WIN_MARK_FIRST_MISSING_IMPL;
    }
}
