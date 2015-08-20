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
import java.util.Set;

import org.apache.commons.lang3.mutable.Mutable;

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
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * Removes duplicate variables from a group-by operator's decor list.
 */
public class RemoveRedundantGroupByDecorVars implements IAlgebraicRewriteRule {

    private final Set<LogicalVariable> vars = new HashSet<LogicalVariable>();
    
    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.GROUP) {
            return false;
        }
        if (context.checkIfInDontApplySet(this, op)) {
            return false;
        }
        vars.clear();
        
        boolean modified = false;
        GroupByOperator groupOp = (GroupByOperator) op;
        Iterator<Pair<LogicalVariable, Mutable<ILogicalExpression>>> iter = groupOp.getDecorList().iterator();
        while (iter.hasNext()) {
            Pair<LogicalVariable, Mutable<ILogicalExpression>> decor = iter.next();
            if (decor.first != null || decor.second.getValue().getExpressionTag() != LogicalExpressionTag.VARIABLE) {
                continue;
            }
            VariableReferenceExpression varRefExpr = (VariableReferenceExpression) decor.second.getValue();
            LogicalVariable var = varRefExpr.getVariableReference();
            if (vars.contains(var)) {
                iter.remove();
                modified = true;
            } else {
                vars.add(var);
            }
        }
        if (modified) {
            context.addToDontApplySet(this, op);
        }
        return modified;
    }

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
        return false;
    }
}
