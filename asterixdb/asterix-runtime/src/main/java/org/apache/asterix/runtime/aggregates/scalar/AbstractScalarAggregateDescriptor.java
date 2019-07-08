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

import java.util.function.Supplier;

import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.typecomputer.impl.TypeComputeUtils;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.aggregates.base.AbstractAggregateFunctionDynamicDescriptor;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.functions.FunctionTypeInferers;
import org.apache.asterix.runtime.unnestingfunctions.std.ScanCollectionDescriptor.ScanCollectionUnnestingFunctionFactory;
import org.apache.asterix.runtime.utils.DescriptorFactoryUtil;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.runtime.base.IAggregateEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IAggregateEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.evaluators.ColumnAccessEvalFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;

public abstract class AbstractScalarAggregateDescriptor extends AbstractScalarFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;

    protected final AbstractAggregateFunctionDynamicDescriptor aggFuncDesc;

    public AbstractScalarAggregateDescriptor(IFunctionDescriptorFactory aggFuncDescFactory) {
        this.aggFuncDesc = (AbstractAggregateFunctionDynamicDescriptor) aggFuncDescFactory.createFunctionDescriptor();
    }

    @Override
    public void setSourceLocation(SourceLocation sourceLoc) {
        super.setSourceLocation(sourceLoc);
        aggFuncDesc.setSourceLocation(sourceLoc);
    }

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args)
            throws AlgebricksException {

        // The aggregate function will get a SingleFieldFrameTupleReference that points to the result of the
        // ScanCollection. The list-item will always reside in the first field (column) of the
        // SingleFieldFrameTupleReference.
        int numArgs = args.length;
        IScalarEvaluatorFactory[] aggFuncArgs = new IScalarEvaluatorFactory[numArgs];

        aggFuncArgs[0] = new ColumnAccessEvalFactory(0);

        for (int i = 1; i < numArgs; ++i) {
            aggFuncArgs[i] = args[i];
        }
        // Create aggregate function from this scalar version.
        final IAggregateEvaluatorFactory aggFuncFactory = aggFuncDesc.createAggregateEvaluatorFactory(aggFuncArgs);

        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(IEvaluatorContext ctx) throws HyracksDataException {
                // Use ScanCollection to iterate over list items.
                ScanCollectionUnnestingFunctionFactory scanCollectionFactory =
                        new ScanCollectionUnnestingFunctionFactory(args[0], sourceLoc);
                return createScalarAggregateEvaluator(aggFuncFactory.createAggregateEvaluator(ctx),
                        scanCollectionFactory, ctx);
            }
        };
    }

    protected IScalarEvaluator createScalarAggregateEvaluator(IAggregateEvaluator aggEval,
            ScanCollectionUnnestingFunctionFactory scanCollectionFactory, IEvaluatorContext ctx)
            throws HyracksDataException {
        return new GenericScalarAggregateFunction(aggEval, scanCollectionFactory, ctx, sourceLoc);
    }

    static IAType getItemType(IAType listType) {
        IAType itemType = TypeComputeUtils.extractListItemType(listType);
        return itemType != null ? itemType : BuiltinType.ANY;
    }

    public static IFunctionDescriptorFactory createDescriptorFactory(Supplier<IFunctionDescriptor> descriptorSupplier) {
        return DescriptorFactoryUtil.createFactory(descriptorSupplier, FunctionTypeInferers.SET_ARGUMENT_TYPE);
    }
}
