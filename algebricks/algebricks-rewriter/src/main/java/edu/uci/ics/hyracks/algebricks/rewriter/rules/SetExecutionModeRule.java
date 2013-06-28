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

import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.hyracks.algebricks.common.exceptions.NotImplementedException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * This rule sets the executionMode property of an operator, w/o introducing
 * EXCHANGE operators in the plan. Previously, i.e. before having physical
 * optimizations in place, we were using the IntroduceExchangeRule, which was
 * doing both, to both set excutionMode and introduce data exchange ops.
 * 
 * @author Nicola
 */
public class SetExecutionModeRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        boolean changed = OperatorManipulationUtil.setOperatorMode(op);
        if (op.getExecutionMode() == AbstractLogicalOperator.ExecutionMode.UNPARTITIONED
                || op.getExecutionMode() == AbstractLogicalOperator.ExecutionMode.LOCAL) {
            return changed;
        }
        switch (op.getOperatorTag()) {
        // case DISTINCT:
        // case AGGREGATE:
        // case GROUP:
        // case ORDER:
        // case INNERJOIN:
        // case LEFTOUTERJOIN: {
        // op.setExecutionMode(ExecutionMode.GLOBAL);
        // return true;
        // }

            case PARTITIONINGSPLIT: {
                throw new NotImplementedException();
            }
            default: {
                return changed;
            }
        }

    }

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        return false;
    }

}
