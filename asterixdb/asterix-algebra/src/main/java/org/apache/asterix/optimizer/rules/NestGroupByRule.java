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
package org.apache.asterix.optimizer.rules;

import java.util.HashSet;
import java.util.Set;

import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.NestedTupleSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class NestGroupByRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op1 = (AbstractLogicalOperator) opRef.getValue();
        if (op1.getOperatorTag() != LogicalOperatorTag.SUBPLAN) {
            return false;
        }
        SubplanOperator subplan = (SubplanOperator) op1;
        if (subplan.getNestedPlans().size() != 1) {
            return false;
        }
        ILogicalPlan p = subplan.getNestedPlans().get(0);
        if (p.getRoots().size() != 1) {
            return false;
        }

        Set<LogicalVariable> free = new HashSet<LogicalVariable>();
        OperatorPropertiesUtil.getFreeVariablesInSubplans(subplan, free);
        if (free.size() != 1) {
            return false;
        }
        LogicalVariable fVar = null;
        for (LogicalVariable v : free) {
            fVar = v;
            break;
        }

        AbstractLogicalOperator op2 = (AbstractLogicalOperator) op1.getInputs().get(0).getValue();
        if (op2.getOperatorTag() != LogicalOperatorTag.GROUP) {
            return false;
        }
        GroupByOperator gby = (GroupByOperator) op2;
        if (gby.getNestedPlans().size() != 1) {
            return false;
        }
        ILogicalPlan p2 = gby.getNestedPlans().get(0);
        if (p2.getRoots().size() != 1) {
            return false;
        }
        Mutable<ILogicalOperator> r2 = p2.getRoots().get(0);
        AbstractLogicalOperator opr2 = (AbstractLogicalOperator) r2.getValue();
        if (opr2.getOperatorTag() != LogicalOperatorTag.AGGREGATE) {
            return false;
        }
        AggregateOperator aggOuter = (AggregateOperator) opr2;
        int posInAggList = aggOuter.getVariables().indexOf(fVar);
        if (posInAggList < 0) {
            return false;
        }
        AbstractLogicalOperator outerAggSon = (AbstractLogicalOperator) aggOuter.getInputs().get(0).getValue();
        if (outerAggSon.getOperatorTag() != LogicalOperatorTag.NESTEDTUPLESOURCE) {
            return false;
        }
        ILogicalExpression eAgg = aggOuter.getExpressions().get(posInAggList).getValue();
        if (eAgg.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }
        AbstractFunctionCallExpression listifyCall = (AbstractFunctionCallExpression) eAgg;
        if (listifyCall.getFunctionIdentifier() != BuiltinFunctions.LISTIFY) {
            return false;
        }
        ILogicalExpression argListify = listifyCall.getArguments().get(0).getValue();
        if (argListify.getExpressionTag() != LogicalExpressionTag.VARIABLE) {
            return false;
        }

        Mutable<ILogicalOperator> r = p.getRoots().get(0);
        AbstractLogicalOperator opInS = (AbstractLogicalOperator) r.getValue();
        if (opInS.getOperatorTag() != LogicalOperatorTag.AGGREGATE) {
            return false;
        }
        AggregateOperator aggInner = (AggregateOperator) opInS;
        do {
            opInS = (AbstractLogicalOperator) opInS.getInputs().get(0).getValue();
        } while (opInS.getOperatorTag() == LogicalOperatorTag.ASSIGN);
        if (opInS.getOperatorTag() != LogicalOperatorTag.GROUP) {
            return false;
        }
        AbstractLogicalOperator unnestParent = opInS;
        AbstractLogicalOperator opUnder = (AbstractLogicalOperator) opInS.getInputs().get(0).getValue();
        // skip Assigns
        while (opUnder.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
            unnestParent = opUnder;
            opUnder = (AbstractLogicalOperator) opUnder.getInputs().get(0).getValue();
        }
        if (opUnder.getOperatorTag() != LogicalOperatorTag.UNNEST) {
            return false;
        }
        UnnestOperator unnest = (UnnestOperator) opUnder;
        AbstractLogicalOperator unnestSon = (AbstractLogicalOperator) unnest.getInputs().get(0).getValue();
        if (unnestSon.getOperatorTag() != LogicalOperatorTag.NESTEDTUPLESOURCE) {
            return false;
        }
        NestedTupleSourceOperator innerNts = (NestedTupleSourceOperator) unnestSon;

        ILogicalExpression eUnnest = unnest.getExpressionRef().getValue();
        if (eUnnest.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }
        AbstractFunctionCallExpression uf = (AbstractFunctionCallExpression) eUnnest;
        if (uf.getFunctionIdentifier() != BuiltinFunctions.SCAN_COLLECTION) {
            return false;
        }
        ILogicalExpression scanArg = uf.getArguments().get(0).getValue();
        if (scanArg.getExpressionTag() != LogicalExpressionTag.VARIABLE) {
            return false;
        }
        if (((VariableReferenceExpression) scanArg).getVariableReference() != fVar) {
            return false;
        }
        LogicalVariable uVar = unnest.getVariable();
        GroupByOperator innerGby = (GroupByOperator) opInS;
        Set<LogicalVariable> freeInInnerGby = new HashSet<LogicalVariable>();
        OperatorPropertiesUtil.getFreeVariablesInSubplans(innerGby, freeInInnerGby);
        for (LogicalVariable v : freeInInnerGby) {
            if (v != uVar) {
                return false;
            }
        }

        unnestParent.getInputs().get(0).setValue(innerNts);
        LogicalVariable listifiedVar = ((VariableReferenceExpression) argListify).getVariableReference();
        substInSubplan(aggInner, uVar, listifiedVar, context);
        gby.getNestedPlans().add(p);
        innerNts.getDataSourceReference().setValue(gby);
        opRef.setValue(gby);
        OperatorPropertiesUtil.typePlan(p, context);
        OperatorPropertiesUtil.typePlan(p2, context);
        context.computeAndSetTypeEnvironmentForOperator(gby);
        return true;

    }

    private void substInSubplan(AggregateOperator aggInner, LogicalVariable v1, LogicalVariable v2,
            IOptimizationContext context) throws AlgebricksException {
        ILogicalOperator op = aggInner;
        while (op.getInputs().size() == 1) {
            VariableUtilities.substituteVariables(op, v1, v2, context);
            op = op.getInputs().get(0).getValue();
        }
    }
}
