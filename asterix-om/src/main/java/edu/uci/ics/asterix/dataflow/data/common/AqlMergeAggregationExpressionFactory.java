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
package edu.uci.ics.asterix.dataflow.data.common;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AggregateFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IMergeAggregationExpressionFactory;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;

public class AqlMergeAggregationExpressionFactory implements IMergeAggregationExpressionFactory {

    @Override
    public ILogicalExpression createMergeAggregation(LogicalVariable originalProducedVar, ILogicalExpression expr,
            IOptimizationContext env) throws AlgebricksException {
        AggregateFunctionCallExpression agg = (AggregateFunctionCallExpression) expr;
        FunctionIdentifier fid = agg.getFunctionIdentifier();
        VariableReferenceExpression tempVarExpr = new VariableReferenceExpression(originalProducedVar);
        List<Mutable<ILogicalExpression>> arguments = new ArrayList<Mutable<ILogicalExpression>>();
        Mutable<ILogicalExpression> mutableExpression = new MutableObject<ILogicalExpression>(tempVarExpr);
        arguments.add(mutableExpression);
        /**
         * For global aggregate, the merge function is ALWAYS the same as the original aggregate function.
         */
        FunctionIdentifier mergeFid = AsterixBuiltinFunctions.isGlobalAggregateFunction(fid) ? fid
                : AsterixBuiltinFunctions.getIntermediateAggregateFunction(fid);
        if (mergeFid == null) {
            /**
             * In this case, no merge function (unimplemented) for the local-side aggregate function
             */
            return null;
        }
        ILogicalExpression aggExpr = AsterixBuiltinFunctions.makeAggregateFunctionExpression(mergeFid, arguments);
        return aggExpr;
    }
}
