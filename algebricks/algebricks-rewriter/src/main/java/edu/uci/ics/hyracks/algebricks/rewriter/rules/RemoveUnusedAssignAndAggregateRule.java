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
package edu.uci.ics.hyracks.algebricks.rewriter.rules;

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.utils.Triple;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractOperatorWithNestedPlans;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.UnionAllOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * Removes unused variables from Assign, Unnest, Aggregate, and UnionAll operators.
 */
public class RemoveUnusedAssignAndAggregateRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        return false;
    }

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
        if (context.checkIfInDontApplySet(this, opRef.getValue())) {
            return false;
        }
        Set<LogicalVariable> toRemove = new HashSet<LogicalVariable>();
        collectUnusedAssignedVars((AbstractLogicalOperator) opRef.getValue(), toRemove, true, context);
        boolean smthToRemove = !toRemove.isEmpty();
        if (smthToRemove) {
            removeUnusedAssigns(opRef, toRemove, context);
        }
        return !toRemove.isEmpty();
    }

    private void removeUnusedAssigns(Mutable<ILogicalOperator> opRef, Set<LogicalVariable> toRemove,
            IOptimizationContext context) throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        while (removeFromAssigns(op, toRemove, context) == 0) {
            if (op.getOperatorTag() == LogicalOperatorTag.AGGREGATE) {
                break;
            }
            op = (AbstractLogicalOperator) op.getInputs().get(0).getValue();
            opRef.setValue(op);
        }
        Iterator<Mutable<ILogicalOperator>> childIter = op.getInputs().iterator();
        while (childIter.hasNext()) {
            Mutable<ILogicalOperator> cRef = childIter.next();
            removeUnusedAssigns(cRef, toRemove, context);
        }
        if (op.hasNestedPlans()) {
            AbstractOperatorWithNestedPlans opWithNest = (AbstractOperatorWithNestedPlans) op;
            Iterator<ILogicalPlan> planIter = opWithNest.getNestedPlans().iterator();
            while (planIter.hasNext()) {
                ILogicalPlan p = planIter.next();
                for (Mutable<ILogicalOperator> r : p.getRoots()) {
                    removeUnusedAssigns(r, toRemove, context);
                }
            }
        }
    }

    private int removeFromAssigns(AbstractLogicalOperator op, Set<LogicalVariable> toRemove,
            IOptimizationContext context) throws AlgebricksException {
        switch (op.getOperatorTag()) {
            case ASSIGN: {
                AssignOperator assign = (AssignOperator) op;
                if (removeUnusedVarsAndExprs(toRemove, assign.getVariables(), assign.getExpressions())) {
                    context.computeAndSetTypeEnvironmentForOperator(assign);
                }
                return assign.getVariables().size();
            }
            case AGGREGATE: {
                AggregateOperator agg = (AggregateOperator) op;
                if (removeUnusedVarsAndExprs(toRemove, agg.getVariables(), agg.getExpressions())) {
                    context.computeAndSetTypeEnvironmentForOperator(agg);
                }
                return agg.getVariables().size();
            }
            case UNNEST: {
                UnnestOperator uOp = (UnnestOperator) op;
                LogicalVariable pVar = uOp.getPositionalVariable();
                if (pVar != null && toRemove.contains(pVar)) {
                    uOp.setPositionalVariable(null);
                }
                break;
            }
            case UNIONALL: {
                UnionAllOperator unionOp = (UnionAllOperator) op;
                if (removeUnusedVarsFromUnionAll(unionOp, toRemove)) {
                    context.computeAndSetTypeEnvironmentForOperator(unionOp);
                }
                return unionOp.getVariableMappings().size();
            }
        }
        return -1;
    }

    private boolean removeUnusedVarsFromUnionAll(UnionAllOperator unionOp, Set<LogicalVariable> toRemove) {
        Iterator<Triple<LogicalVariable, LogicalVariable, LogicalVariable>> iter = unionOp.getVariableMappings()
                .iterator();
        boolean modified = false;
        Set<LogicalVariable> removeFromRemoveSet = new HashSet<LogicalVariable>();
        while (iter.hasNext()) {
            Triple<LogicalVariable, LogicalVariable, LogicalVariable> varMapping = iter.next();
            if (toRemove.contains(varMapping.third)) {
                iter.remove();
                modified = true;
            }
            // In any case, make sure we do not removing these variables.
            removeFromRemoveSet.add(varMapping.first);
            removeFromRemoveSet.add(varMapping.second);
        }
        toRemove.removeAll(removeFromRemoveSet);
        return modified;
    }

    private boolean removeUnusedVarsAndExprs(Set<LogicalVariable> toRemove, List<LogicalVariable> varList,
            List<Mutable<ILogicalExpression>> exprList) {
        boolean changed = false;
        Iterator<LogicalVariable> varIter = varList.iterator();
        Iterator<Mutable<ILogicalExpression>> exprIter = exprList.iterator();
        while (varIter.hasNext()) {
            LogicalVariable v = varIter.next();
            exprIter.next();
            if (toRemove.contains(v)) {
                varIter.remove();
                exprIter.remove();
                changed = true;
            }
        }
        return changed;
    }

    private void collectUnusedAssignedVars(AbstractLogicalOperator op, Set<LogicalVariable> toRemove, boolean first,
            IOptimizationContext context) throws AlgebricksException {
        if (!first) {
            context.addToDontApplySet(this, op);
        }
        for (Mutable<ILogicalOperator> c : op.getInputs()) {
            collectUnusedAssignedVars((AbstractLogicalOperator) c.getValue(), toRemove, false, context);
        }
        if (op.hasNestedPlans()) {
            AbstractOperatorWithNestedPlans opWithNested = (AbstractOperatorWithNestedPlans) op;
            for (ILogicalPlan plan : opWithNested.getNestedPlans()) {
                for (Mutable<ILogicalOperator> r : plan.getRoots()) {
                    collectUnusedAssignedVars((AbstractLogicalOperator) r.getValue(), toRemove, false, context);
                }
            }
        }
        boolean removeUsedVars = true;
        switch (op.getOperatorTag()) {
            case ASSIGN: {
                AssignOperator assign = (AssignOperator) op;
                toRemove.addAll(assign.getVariables());
                break;
            }
            case AGGREGATE: {
                AggregateOperator agg = (AggregateOperator) op;
                toRemove.addAll(agg.getVariables());
                break;
            }
            case UNNEST: {
                UnnestOperator uOp = (UnnestOperator) op;
                LogicalVariable pVar = uOp.getPositionalVariable();
                if (pVar != null) {
                    toRemove.add(pVar);
                }
                break;
            }
            case UNIONALL: {
                UnionAllOperator unionOp = (UnionAllOperator) op;
                for (Triple<LogicalVariable, LogicalVariable, LogicalVariable> varMapping : unionOp
                        .getVariableMappings()) {
                    toRemove.add(varMapping.third);
                }
                removeUsedVars = false;
                break;
            }
        }
        if (removeUsedVars) {
            List<LogicalVariable> used = new LinkedList<LogicalVariable>();
            VariableUtilities.getUsedVariables(op, used);
            toRemove.removeAll(used);
        }
    }

}
