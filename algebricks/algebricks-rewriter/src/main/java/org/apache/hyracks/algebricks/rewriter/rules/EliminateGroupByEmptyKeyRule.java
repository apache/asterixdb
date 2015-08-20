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

import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * This rule lift out the aggregate operator out from a group-by operator
 * if the gby operator groups-by on empty key, e.g., the group-by variables are empty.
 * 
 * @author yingyib
 */
public class EliminateGroupByEmptyKeyRule implements IAlgebraicRewriteRule {

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
        GroupByOperator groupOp = (GroupByOperator) op;
        List<LogicalVariable> groupVars = groupOp.getGbyVarList();
        if (groupVars.size() > 0) {
            return false;
        }
        List<ILogicalPlan> nestedPlans = groupOp.getNestedPlans();
        if (nestedPlans.size() > 1) {
            return false;
        }
        ILogicalPlan nestedPlan = nestedPlans.get(0);
        if (nestedPlan.getRoots().size() > 1) {
            return false;
        }
        Mutable<ILogicalOperator> topOpRef = nestedPlan.getRoots().get(0);
        ILogicalOperator topOp = nestedPlan.getRoots().get(0).getValue();
        Mutable<ILogicalOperator> nestedTupleSourceRef = getNestedTupleSourceReference(topOpRef);
        /**
         * connect nested top op into the plan
         */
        opRef.setValue(topOp);
        /**
         * connect child op into the plan
         */
        nestedTupleSourceRef.setValue(groupOp.getInputs().get(0).getValue());
        return true;
    }

    private Mutable<ILogicalOperator> getNestedTupleSourceReference(Mutable<ILogicalOperator> nestedTopOperatorRef) {
        Mutable<ILogicalOperator> currentOpRef = nestedTopOperatorRef;
        while (currentOpRef.getValue().getInputs() != null && currentOpRef.getValue().getInputs().size() > 0) {
            currentOpRef = currentOpRef.getValue().getInputs().get(0);
        }
        return currentOpRef;
    }

}
