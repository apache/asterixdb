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
package org.apache.asterix.runtime.aggregates.scalar;

import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionManager;
import org.apache.asterix.runtime.aggregates.base.AbstractAggregateFunctionDynamicDescriptor;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.functions.FunctionManagerHolder;
import org.apache.asterix.runtime.unnestingfunctions.std.ScanCollectionDescriptor.ScanCollectionUnnestingFunctionFactory;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IAggregateEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.evaluators.ColumnAccessEvalFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public abstract class AbstractScalarAggregateDescriptor extends AbstractScalarFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args)
            throws AlgebricksException {
        // The aggregate function will get a SingleFieldFrameTupleReference that points to the result of the ScanCollection.
        // The list-item will always reside in the first field (column) of the SingleFieldFrameTupleReference.
        IScalarEvaluatorFactory[] aggFuncArgs = new IScalarEvaluatorFactory[1];
        aggFuncArgs[0] = new ColumnAccessEvalFactory(0);
        // Create aggregate function from this scalar version.
        FunctionIdentifier fid = BuiltinFunctions.getAggregateFunction(getIdentifier());
        IFunctionManager mgr = FunctionManagerHolder.getFunctionManager();
        IFunctionDescriptor fd = mgr.lookupFunction(fid);
        AbstractAggregateFunctionDynamicDescriptor aggFuncDesc = (AbstractAggregateFunctionDynamicDescriptor) fd;
        final IAggregateEvaluatorFactory aggFuncFactory = aggFuncDesc.createAggregateEvaluatorFactory(aggFuncArgs);

        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(IHyracksTaskContext ctx) throws HyracksDataException {
                // Use ScanCollection to iterate over list items.
                ScanCollectionUnnestingFunctionFactory scanCollectionFactory = new ScanCollectionUnnestingFunctionFactory(
                        args[0]);
                return new GenericScalarAggregateFunction(aggFuncFactory.createAggregateEvaluator(ctx),
                        scanCollectionFactory, ctx);
            }
        };
    }
}
