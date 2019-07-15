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
package org.apache.hyracks.algebricks.rewriter.rules;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AggregateFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator.ExecutionMode;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.NestedTupleSourceOperator;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import org.apache.hyracks.api.exceptions.SourceLocation;

public abstract class AbstractIntroduceCombinerRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        return false;
    }

    /**
     * Replace the original aggregate functions with their corresponding global aggregate function.
     */
    protected void replaceOriginalAggFuncs(Set<SimilarAggregatesInfo> toReplaceSet) {
        for (SimilarAggregatesInfo sai : toReplaceSet) {
            for (AggregateExprInfo aei : sai.simAggs) {
                AbstractFunctionCallExpression afce = (AbstractFunctionCallExpression) aei.aggExprRef.getValue();
                afce.setFunctionInfo(aei.newFunInfo);
                List<Mutable<ILogicalExpression>> args = new ArrayList<Mutable<ILogicalExpression>>();
                args.addAll(afce.getArguments());
                afce.getArguments().clear();
                afce.getArguments().add(new MutableObject<>(sai.stepOneResult));
                afce.getArguments().addAll(args.subList(1, args.size()));
            }
        }
    }

    protected Pair<Boolean, Mutable<ILogicalOperator>> tryToPushAgg(AggregateOperator initAgg, GroupByOperator newGbyOp,
            Set<SimilarAggregatesInfo> toReplaceSet, IOptimizationContext context) throws AlgebricksException {
        SourceLocation sourceLoc = initAgg.getSourceLocation();
        List<LogicalVariable> initVars = initAgg.getVariables();
        List<Mutable<ILogicalExpression>> initExprs = initAgg.getExpressions();
        int numExprs = initVars.size();

        // First make sure that all agg funcs are two step, otherwise we cannot use local aggs.
        for (int i = 0; i < numExprs; i++) {
            AggregateFunctionCallExpression aggFun = (AggregateFunctionCallExpression) initExprs.get(i).getValue();
            if (!aggFun.isTwoStep()) {
                return new Pair<>(false, null);
            }
        }

        ArrayList<LogicalVariable> pushedVars = new ArrayList<>();
        ArrayList<Mutable<ILogicalExpression>> pushedExprs = new ArrayList<>();

        boolean haveAggToReplace = false;
        for (int i = 0; i < numExprs; i++) {
            Mutable<ILogicalExpression> expRef = initExprs.get(i);
            AggregateFunctionCallExpression aggFun = (AggregateFunctionCallExpression) expRef.getValue();
            SourceLocation aggFunSourceLoc = aggFun.getSourceLocation();
            IFunctionInfo fi1 = aggFun.getStepOneAggregate();
            // Clone the aggregate's args.
            List<Mutable<ILogicalExpression>> newArgs = new ArrayList<>(aggFun.getArguments().size());
            for (Mutable<ILogicalExpression> er : aggFun.getArguments()) {
                newArgs.add(new MutableObject<>(er.getValue().cloneExpression()));
            }
            IFunctionInfo fi2 = aggFun.getStepTwoAggregate();

            SimilarAggregatesInfo inf = new SimilarAggregatesInfo();
            LogicalVariable newAggVar = context.newVar();
            pushedVars.add(newAggVar);
            VariableReferenceExpression newAggVarRef = new VariableReferenceExpression(newAggVar);
            newAggVarRef.setSourceLocation(aggFunSourceLoc);
            inf.stepOneResult = newAggVarRef;
            inf.simAggs = new ArrayList<>();
            toReplaceSet.add(inf);
            AggregateFunctionCallExpression aggLocal = new AggregateFunctionCallExpression(fi1, false, newArgs);
            aggLocal.setSourceLocation(aggFunSourceLoc);
            pushedExprs.add(new MutableObject<>(aggLocal));
            AggregateExprInfo aei = new AggregateExprInfo();
            aei.aggExprRef = expRef;
            aei.newFunInfo = fi2;
            inf.simAggs.add(aei);
            haveAggToReplace = true;
        }

        if (!pushedVars.isEmpty()) {
            AggregateOperator pushedAgg = new AggregateOperator(pushedVars, pushedExprs);
            pushedAgg.setSourceLocation(sourceLoc);
            pushedAgg.setExecutionMode(ExecutionMode.LOCAL);
            // If newGbyOp is null, then we optimizing an aggregate without group by.
            if (newGbyOp != null) {
                // Cut and paste nested input pipelines of initAgg to pushedAgg's input
                Mutable<ILogicalOperator> inputRef = initAgg.getInputs().get(0);
                if (!isPushableInputInGroupBySubplan(inputRef.getValue())) {
                    return new Pair<>(false, null);
                }
                Mutable<ILogicalOperator> bottomRef = inputRef;
                while (!bottomRef.getValue().getInputs().isEmpty()) {
                    bottomRef = bottomRef.getValue().getInputs().get(0);
                    if (!isPushableInputInGroupBySubplan(bottomRef.getValue())) {
                        return new Pair<>(false, null);
                    }
                }
                ILogicalOperator oldNts = bottomRef.getValue();
                initAgg.getInputs().clear();
                initAgg.getInputs().add(new MutableObject<>(oldNts));

                // Hook up the nested aggregate op with the outer group by.
                NestedTupleSourceOperator nts = new NestedTupleSourceOperator(new MutableObject<>(newGbyOp));
                nts.setSourceLocation(sourceLoc);
                nts.setExecutionMode(ExecutionMode.LOCAL);
                bottomRef.setValue(nts);
                // is there a reason we are not setting pushedAgg.setGlobal(false)?
                pushedAgg.getInputs().add(inputRef);
            } else {
                // The local aggregate operator is fed by the input of the original aggregate operator.
                pushedAgg.getInputs().add(new MutableObject<>(initAgg.getInputs().get(0).getValue()));
                // Reintroduce assign op for the global agg partitioning var.
                initAgg.getInputs().get(0).setValue(pushedAgg);
                pushedAgg.setGlobal(false);
                context.computeAndSetTypeEnvironmentForOperator(pushedAgg);
            }
            return new Pair<>(true, new MutableObject<ILogicalOperator>(pushedAgg));
        } else {
            return new Pair<>(haveAggToReplace, null);
        }
    }

    protected boolean isPushableInputInGroupBySubplan(ILogicalOperator op) {
        switch (op.getOperatorTag()) {
            case ASSIGN:
            case NESTEDTUPLESOURCE:
            case ORDER:
            case PROJECT:
            case SELECT:
                return true;
            default:
                return false;
        }
    }

    protected class SimilarAggregatesInfo {
        ILogicalExpression stepOneResult;
        List<AggregateExprInfo> simAggs;
    }

    protected class AggregateExprInfo {
        Mutable<ILogicalExpression> aggExprRef;
        IFunctionInfo newFunInfo;
    }

    protected class BookkeepingInfo {
        Map<GroupByOperator, List<LogicalVariable>> modifyGbyMap = new HashMap<>();
    }
}
