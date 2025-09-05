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
import org.apache.hyracks.algebricks.runtime.base.IRunningAggregateEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IRunningAggregateEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IEvaluatorContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * Descriptor for WIN_MARK_VALID_TUPLES_IMPL function.
 *
 * Marks tuples as valid in window operations. This is
 * used for detecting the Halloween problem in index-only
 * query plans. Returns true for tuples that are valid
 * (valid tuples are those that have the same values as
 * the first tuple, and anything that has a value other
 * than the first tuple is considered invalid).
 */
public class WinMarkValidTuplesRunningAggregateDescriptor extends AbstractRunningAggregateFunctionDynamicDescriptor {

    public static final IFunctionDescriptorFactory FACTORY = WinMarkValidTuplesRunningAggregateDescriptor::new;

    private static final long serialVersionUID = 1L;

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
                return new WinMarkValidTuplesRunningAggregateEvaluator(evals);
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.WIN_MARK_VALID_TUPLES_IMPL;
    }
}
