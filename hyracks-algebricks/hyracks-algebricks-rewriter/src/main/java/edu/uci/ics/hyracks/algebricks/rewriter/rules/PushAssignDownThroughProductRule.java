/*
 * Copyright 2009-2010 by The Regents of the University of California
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

import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorReference;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class PushAssignDownThroughProductRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(LogicalOperatorReference opRef, IOptimizationContext context) throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePost(LogicalOperatorReference opRef, IOptimizationContext context) throws AlgebricksException {
        AbstractLogicalOperator op1 = (AbstractLogicalOperator) opRef.getOperator();
        if (op1.getOperatorTag() != LogicalOperatorTag.ASSIGN) {
            return false;
        }
        LogicalOperatorReference op2Ref = op1.getInputs().get(0);
        AbstractLogicalOperator op2 = (AbstractLogicalOperator) op2Ref.getOperator();
        if (op2.getOperatorTag() != LogicalOperatorTag.INNERJOIN) {
            return false;
        }
        AbstractBinaryJoinOperator join = (AbstractBinaryJoinOperator) op2;
        if (join.getCondition().getExpression() != ConstantExpression.TRUE) {
            return false;
        }

        List<LogicalVariable> used = new ArrayList<LogicalVariable>();
        VariableUtilities.getUsedVariables(op1, used);

        LogicalOperatorReference b0Ref = op2.getInputs().get(0);
        ILogicalOperator b0 = b0Ref.getOperator();
        List<LogicalVariable> b0Scm = new ArrayList<LogicalVariable>();
        VariableUtilities.getLiveVariables(b0, b0Scm);
        if (b0Scm.containsAll(used)) {
            // push assign on left branch
            op2Ref.setOperator(b0);
            b0Ref.setOperator(op1);
            opRef.setOperator(op2);
            return true;
        } else {
            LogicalOperatorReference b1Ref = op2.getInputs().get(1);
            ILogicalOperator b1 = b1Ref.getOperator();
            List<LogicalVariable> b1Scm = new ArrayList<LogicalVariable>();
            VariableUtilities.getLiveVariables(b1, b1Scm);
            if (b1Scm.containsAll(used)) {
                // push assign on right branch
                op2Ref.setOperator(b1);
                b1Ref.setOperator(op1);
                opRef.setOperator(op2);
                return true;
            } else {
                return false;
            }
        }
    }

}
