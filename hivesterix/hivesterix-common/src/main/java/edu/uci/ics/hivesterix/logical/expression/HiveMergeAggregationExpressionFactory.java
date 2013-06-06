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
package edu.uci.ics.hivesterix.logical.expression;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AggregateFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IMergeAggregationExpressionFactory;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;

/**
 * generate merge aggregation expression from an aggregation expression
 * 
 * @author yingyib
 */
public class HiveMergeAggregationExpressionFactory implements IMergeAggregationExpressionFactory {

    public static IMergeAggregationExpressionFactory INSTANCE = new HiveMergeAggregationExpressionFactory();

    @Override
    public ILogicalExpression createMergeAggregation(ILogicalExpression expr, IOptimizationContext context)
            throws AlgebricksException {
        /**
         * type inference for scalar function
         */
        if (expr instanceof AggregateFunctionCallExpression) {
            AggregateFunctionCallExpression funcExpr = (AggregateFunctionCallExpression) expr;
            /**
             * hive aggregation info
             */
            AggregationDesc aggregator = (AggregationDesc) ((HiveFunctionInfo) funcExpr.getFunctionInfo()).getInfo();
            LogicalVariable inputVar = context.newVar();
            ExprNodeDesc col = new ExprNodeColumnDesc(TypeInfoFactory.voidTypeInfo, inputVar.toString(), null, false);
            ArrayList<ExprNodeDesc> parameters = new ArrayList<ExprNodeDesc>();
            parameters.add(col);

            GenericUDAFEvaluator.Mode mergeMode;
            if (aggregator.getMode() == GenericUDAFEvaluator.Mode.PARTIAL1)
                mergeMode = GenericUDAFEvaluator.Mode.PARTIAL2;
            else if (aggregator.getMode() == GenericUDAFEvaluator.Mode.COMPLETE)
                mergeMode = GenericUDAFEvaluator.Mode.FINAL;
            else
                mergeMode = aggregator.getMode();
            AggregationDesc mergeDesc = new AggregationDesc(aggregator.getGenericUDAFName(),
                    aggregator.getGenericUDAFEvaluator(), parameters, aggregator.getDistinct(), mergeMode);

            String UDAFName = mergeDesc.getGenericUDAFName();
            List<Mutable<ILogicalExpression>> arguments = new ArrayList<Mutable<ILogicalExpression>>();
            arguments.add(new MutableObject<ILogicalExpression>(new VariableReferenceExpression(inputVar)));

            FunctionIdentifier funcId = new FunctionIdentifier(ExpressionConstant.NAMESPACE, UDAFName + "("
                    + mergeDesc.getMode() + ")");
            HiveFunctionInfo funcInfo = new HiveFunctionInfo(funcId, mergeDesc);
            AggregateFunctionCallExpression aggregationExpression = new AggregateFunctionCallExpression(funcInfo,
                    false, arguments);
            return aggregationExpression;
        } else {
            throw new IllegalStateException("illegal expressions " + expr.getClass().getName());
        }
    }

}
