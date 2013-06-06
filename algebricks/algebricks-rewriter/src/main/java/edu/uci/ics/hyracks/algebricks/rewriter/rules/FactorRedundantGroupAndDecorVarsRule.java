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

import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class FactorRedundantGroupAndDecorVarsRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.GROUP) {
            return false;
        }
        GroupByOperator gby = (GroupByOperator) op;
        Map<LogicalVariable, LogicalVariable> varRhsToLhs = new HashMap<LogicalVariable, LogicalVariable>();
        boolean gvChanged = factorRedundantRhsVars(gby.getGroupByList(), opRef, varRhsToLhs, context);
        boolean dvChanged = factorRedundantRhsVars(gby.getDecorList(), opRef, varRhsToLhs, context);

        return gvChanged || dvChanged;
    }

    private boolean factorRedundantRhsVars(List<Pair<LogicalVariable, Mutable<ILogicalExpression>>> veList,
            Mutable<ILogicalOperator> opRef, Map<LogicalVariable, LogicalVariable> varRhsToLhs,
            IOptimizationContext context) throws AlgebricksException {
        varRhsToLhs.clear();
        ListIterator<Pair<LogicalVariable, Mutable<ILogicalExpression>>> iter = veList.listIterator();
        boolean changed = false;
        while (iter.hasNext()) {
            Pair<LogicalVariable, Mutable<ILogicalExpression>> p = iter.next();
            if (p.second.getValue().getExpressionTag() != LogicalExpressionTag.VARIABLE) {
                continue;
            }
            LogicalVariable v = GroupByOperator.getDecorVariable(p);
            LogicalVariable lhs = varRhsToLhs.get(v);
            if (lhs != null) {
                if (p.first != null) {
                    AssignOperator assign = new AssignOperator(p.first, new MutableObject<ILogicalExpression>(
                            new VariableReferenceExpression(lhs)));
                    ILogicalOperator op = opRef.getValue();
                    assign.getInputs().add(new MutableObject<ILogicalOperator>(op));
                    opRef.setValue(assign);
                    context.computeAndSetTypeEnvironmentForOperator(assign);
                }
                iter.remove();
                changed = true;
            } else {
                varRhsToLhs.put(v, p.first);
            }
        }
        return changed;
    }

}
