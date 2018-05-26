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
package org.apache.hyracks.algebricks.rewriter.rules.subplan;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractOperatorWithNestedPlans;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.EmptyTupleSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InnerJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import org.apache.hyracks.api.exceptions.SourceLocation;

/**
 * replace Subplan operators with nested loop joins where the join condition is true, if the Subplan
 * does not contain free variables (does not have correlations to the input stream).
 *
 * @author yingyib
 */
public class NestedSubplanToJoinRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        if (context.checkIfInDontApplySet(this, opRef.getValue()))
            return false;
        context.addToDontApplySet(this, opRef.getValue());

        ILogicalOperator op1 = opRef.getValue();
        if (op1.getInputs().size() == 0) {
            return false;
        }

        boolean rewritten = false;
        for (int index = 0; index < op1.getInputs().size(); index++) {
            AbstractLogicalOperator child = (AbstractLogicalOperator) op1.getInputs().get(index).getValue();
            if (child.getOperatorTag() != LogicalOperatorTag.SUBPLAN) {
                continue;
            }

            AbstractOperatorWithNestedPlans subplan = (AbstractOperatorWithNestedPlans) child;
            Set<LogicalVariable> freeVars = new HashSet<LogicalVariable>();
            OperatorPropertiesUtil.getFreeVariablesInSubplans(subplan, freeVars);
            if (!freeVars.isEmpty()) {
                /**
                 * the subplan is correlated with the outer plan, other rules can deal with it
                 */
                continue;
            }

            /** get the input operator of the subplan operator */
            ILogicalOperator subplanInput = subplan.getInputs().get(0).getValue();
            AbstractLogicalOperator subplanInputOp = (AbstractLogicalOperator) subplanInput;

            /** If the other join branch is a trivial plan, do not do the rewriting. */
            if (subplanInputOp.getOperatorTag() == LogicalOperatorTag.EMPTYTUPLESOURCE) {
                continue;
            }

            /** get all nested top operators */
            List<ILogicalPlan> nestedPlans = subplan.getNestedPlans();
            List<Mutable<ILogicalOperator>> nestedRoots = new ArrayList<Mutable<ILogicalOperator>>();
            for (ILogicalPlan nestedPlan : nestedPlans) {
                nestedRoots.addAll(nestedPlan.getRoots());
            }
            if (nestedRoots.size() == 0) {
                /** there is no nested top operators */
                continue;
            }

            SourceLocation sourceLoc = subplan.getSourceLocation();

            /**
             * Expends the input and roots into a DAG of nested loop joins.
             * Though joins should be left-outer joins, a left-outer join with condition TRUE is equivalent to an inner join.
             **/
            Mutable<ILogicalExpression> expr = new MutableObject<ILogicalExpression>(ConstantExpression.TRUE);
            Mutable<ILogicalOperator> nestedRootRef = nestedRoots.get(0);
            InnerJoinOperator join =
                    new InnerJoinOperator(expr, new MutableObject<ILogicalOperator>(subplanInput), nestedRootRef);
            join.setSourceLocation(sourceLoc);

            /** rewrite the nested tuple source to be empty tuple source */
            rewriteNestedTupleSource(nestedRootRef, context);

            for (int i = 1; i < nestedRoots.size(); i++) {
                join = new InnerJoinOperator(expr, new MutableObject<ILogicalOperator>(join), nestedRoots.get(i));
                join.setSourceLocation(sourceLoc);
            }
            op1.getInputs().get(index).setValue(join);
            context.computeAndSetTypeEnvironmentForOperator(join);
            rewritten = true;
        }
        return rewritten;
    }

    /**
     * rewrite NestedTupleSource operators to EmptyTupleSource operators
     *
     * @param nestedRootRef
     */
    private void rewriteNestedTupleSource(Mutable<ILogicalOperator> nestedRootRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator nestedRoot = (AbstractLogicalOperator) nestedRootRef.getValue();
        if (nestedRoot.getOperatorTag() == LogicalOperatorTag.NESTEDTUPLESOURCE) {
            ILogicalOperator ets = new EmptyTupleSourceOperator();
            nestedRootRef.setValue(ets);
            context.computeAndSetTypeEnvironmentForOperator(ets);
        }
        List<Mutable<ILogicalOperator>> inputs = nestedRoot.getInputs();
        for (Mutable<ILogicalOperator> input : inputs) {
            rewriteNestedTupleSource(input, context);
        }
    }
}
