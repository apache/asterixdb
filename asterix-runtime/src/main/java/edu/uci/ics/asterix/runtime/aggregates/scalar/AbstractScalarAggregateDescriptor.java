/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.runtime.aggregates.scalar;

import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.functions.FunctionManagerHolder;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptor;
import edu.uci.ics.asterix.om.functions.IFunctionManager;
import edu.uci.ics.asterix.runtime.aggregates.base.AbstractAggregateFunctionDynamicDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import edu.uci.ics.asterix.runtime.unnestingfunctions.std.ScanCollectionDescriptor.ScanCollectionUnnestingFunctionFactory;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyAggregateFunctionFactory;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.runtime.evaluators.ColumnAccessEvalFactory;
import edu.uci.ics.hyracks.data.std.api.IDataOutputProvider;

public abstract class AbstractScalarAggregateDescriptor extends AbstractScalarFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;

    @Override
    public ICopyEvaluatorFactory createEvaluatorFactory(final ICopyEvaluatorFactory[] args) throws AlgebricksException {

        // The aggregate function will get a SingleFieldFrameTupleReference that points to the result of the ScanCollection.
        // The list-item will always reside in the first field (column) of the SingleFieldFrameTupleReference.
        ICopyEvaluatorFactory[] aggFuncArgs = new ICopyEvaluatorFactory[1];
        aggFuncArgs[0] = new ColumnAccessEvalFactory(0);
        // Create aggregate function from this scalar version.
        FunctionIdentifier fid = AsterixBuiltinFunctions.getAggregateFunction(getIdentifier());
        IFunctionManager mgr = FunctionManagerHolder.getFunctionManager();
        IFunctionDescriptor fd = mgr.lookupFunction(fid);
        AbstractAggregateFunctionDynamicDescriptor aggFuncDesc = (AbstractAggregateFunctionDynamicDescriptor) fd;
        final ICopyAggregateFunctionFactory aggFuncFactory = aggFuncDesc.createAggregateFunctionFactory(aggFuncArgs);

        return new ICopyEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public ICopyEvaluator createEvaluator(IDataOutputProvider output) throws AlgebricksException {
                // Use ScanCollection to iterate over list items.
                ScanCollectionUnnestingFunctionFactory scanCollectionFactory = new ScanCollectionUnnestingFunctionFactory(
                        args[0]);
                return new GenericScalarAggregateFunction(aggFuncFactory.createAggregateFunction(output),
                        scanCollectionFactory);
            }
        };
    }
}
