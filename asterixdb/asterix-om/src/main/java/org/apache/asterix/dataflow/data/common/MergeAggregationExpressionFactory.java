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
package org.apache.asterix.dataflow.data.common;

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AggregateFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IMergeAggregationExpressionFactory;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;

public class MergeAggregationExpressionFactory implements IMergeAggregationExpressionFactory {

    @Override
    public ILogicalExpression createMergeAggregation(LogicalVariable originalProducedVar, ILogicalExpression expr,
            IOptimizationContext env) throws AlgebricksException {
        AggregateFunctionCallExpression agg = (AggregateFunctionCallExpression) expr;
        FunctionIdentifier fid = agg.getFunctionIdentifier();
        VariableReferenceExpression tempVarExpr = new VariableReferenceExpression(originalProducedVar);
        tempVarExpr.setSourceLocation(agg.getSourceLocation());
        List<Mutable<ILogicalExpression>> arguments = new ArrayList<Mutable<ILogicalExpression>>();
        Mutable<ILogicalExpression> mutableExpression = new MutableObject<ILogicalExpression>(tempVarExpr);
        arguments.add(mutableExpression);
        /**
         * For global aggregate, the merge function is ALWAYS the same as the original aggregate function.
         */
        FunctionIdentifier mergeFid = BuiltinFunctions.isGlobalAggregateFunction(fid) ? fid
                : BuiltinFunctions.getIntermediateAggregateFunction(fid);
        if (mergeFid == null) {
            /**
             * In this case, no merge function (unimplemented) for the local-side aggregate function
             */
            return null;
        }
        AggregateFunctionCallExpression mergeExpr =
                BuiltinFunctions.makeAggregateFunctionExpression(mergeFid, arguments);
        mergeExpr.setSourceLocation(agg.getSourceLocation());
        return mergeExpr;
    }
}
