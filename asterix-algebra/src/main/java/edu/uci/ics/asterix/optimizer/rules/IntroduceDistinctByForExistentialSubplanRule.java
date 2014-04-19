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
package edu.uci.ics.asterix.optimizer.rules;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AggregateFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.DistinctOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.FunctionalDependency;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import edu.uci.ics.hyracks.algebricks.rewriter.util.PhysicalOptimizationsUtil;

/*
 * Before Plan:
 *     select ($x)
 *       subplan {
 *              aggregate [$x] <- [function-call: asterix:non-empty-stream ]
 *                select (cond($y))
 *                  unnest $y <- function-call: asterix:scan-collection
 *                    nested tuple source
 *               }
 *         Rest
 *               
 * After Plan:  
 *     distinct (pk of $y)
 *       select(cond($y))
 *         unnest $y <- function-call: asterix:scan-collection
 *           Rest
 */
public class IntroduceDistinctByForExistentialSubplanRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op0 = (AbstractLogicalOperator) opRef.getValue();
        if (op0.getOperatorTag() != LogicalOperatorTag.SELECT) {
            return false;
        }
        SelectOperator so = (SelectOperator) op0;
        if (so.getCondition().getValue().getExpressionTag() != LogicalExpressionTag.VARIABLE) {
            return false;
        }
        AbstractLogicalOperator op1 = (AbstractLogicalOperator) op0.getInputs().get(0).getValue();
        if (op1.getOperatorTag() != LogicalOperatorTag.SUBPLAN) {
            return false;
        }
        SubplanOperator subplan = (SubplanOperator) op1;
        if (subplan.getNestedPlans().size() != 1 || subplan.getNestedPlans().get(0).getRoots().size() != 1) {
            return false;
        }
        Mutable<ILogicalOperator> subplanRoot = subplan.getNestedPlans().get(0).getRoots().get(0);
        AbstractLogicalOperator op2 = (AbstractLogicalOperator) subplanRoot.getValue();

        if (op2.getOperatorTag() != LogicalOperatorTag.AGGREGATE) {
            return false;
        }
        AggregateOperator aggregate = (AggregateOperator) op2;       
        if (aggregate.getExpressions().size() != 1) {
            return false;
        }
        AggregateFunctionCallExpression aggFun = (AggregateFunctionCallExpression) aggregate.getExpressions().get(0).getValue();
        if (aggFun.getFunctionIdentifier() != AsterixBuiltinFunctions.NON_EMPTY_STREAM) {
            return false;
        }

        AbstractLogicalOperator op3 = (AbstractLogicalOperator) aggregate.getInputs().get(0).getValue();
        if (op3.getOperatorTag() != LogicalOperatorTag.SELECT) {
            return false;
        }
        SelectOperator topSelect = (SelectOperator) op3;
        
        AbstractLogicalOperator prevOp = op3;
        AbstractLogicalOperator curOp;
        do {
            curOp = (AbstractLogicalOperator) prevOp.getInputs().get(0).getValue();
            if (curOp.getOperatorTag() == LogicalOperatorTag.NESTEDTUPLESOURCE) {
                break;
            }
            if (curOp.getOperatorTag() != LogicalOperatorTag.SELECT && curOp.getOperatorTag() != LogicalOperatorTag.UNNEST) {
                return false;
            }
            prevOp = curOp;
        } while (curOp.getInputs().size() == 1);
        
        
        // Compute distinct by variables       
        Set<LogicalVariable> free = new HashSet<LogicalVariable>();
        Set<LogicalVariable> pkVars = computeDistinctByVars(topSelect, free, context);
        if (pkVars == null || pkVars.isEmpty()) {
            return false;
        }
        List<Mutable<ILogicalExpression>> expressions = new ArrayList<Mutable<ILogicalExpression>>();
        for (LogicalVariable v : pkVars) {
            ILogicalExpression varExpr = new VariableReferenceExpression(v);
            expressions.add(new MutableObject<ILogicalExpression>(varExpr));
        }
        DistinctOperator distinct = new DistinctOperator(expressions);
        distinct.getInputs().add(new MutableObject<ILogicalOperator>(topSelect));
        opRef.setValue(distinct);
        
        AbstractLogicalOperator bottomOp = (AbstractLogicalOperator) subplan.getInputs().get(0).getValue();
        List<Mutable<ILogicalOperator>> unnestInputList = prevOp.getInputs();
        unnestInputList.clear();
        unnestInputList.add(new MutableObject<ILogicalOperator>(bottomOp));
        
        context.computeAndSetTypeEnvironmentForOperator(distinct);
        
        return true;
    }
    
    protected Set<LogicalVariable> computeDistinctByVars(AbstractLogicalOperator op, Set<LogicalVariable> freeVars,
            IOptimizationContext context) throws AlgebricksException {
        PhysicalOptimizationsUtil.computeFDsAndEquivalenceClasses(op, context);
        List<FunctionalDependency> fdList = context.getFDList(op);
        if (fdList == null) {
            return null;
        }
        // check if any of the FDs is a key
        List<LogicalVariable> all = new ArrayList<LogicalVariable>();
        VariableUtilities.getLiveVariables(op, all);
        all.retainAll(freeVars);
        for (FunctionalDependency fd : fdList) {
            if (fd.getTail().containsAll(all)) {
                return new HashSet<LogicalVariable>(fd.getHead());
            }
        }
        return null;
    }

}
