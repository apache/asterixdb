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

import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import edu.uci.ics.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class PushSelectDownRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        return false;
    }

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.SELECT) {
            return false;
        }

        Mutable<ILogicalOperator> opRef2 = op.getInputs().get(0);
        AbstractLogicalOperator op2 = (AbstractLogicalOperator) opRef2.getValue();

        if (context.checkAndAddToAlreadyCompared(op, op2)) {
            return false;
        }

        LogicalOperatorTag tag2 = op2.getOperatorTag();

        if (tag2 == LogicalOperatorTag.INNERJOIN || tag2 == LogicalOperatorTag.LEFTOUTERJOIN
                || tag2 == LogicalOperatorTag.REPLICATE) {
            return false;
        } else { // not a join
            boolean res = propagateSelectionRec(opRef, opRef2);
            if (res) {
                OperatorPropertiesUtil.typeOpRec(opRef, context);
            }
            return res;
        }
    }

    private static boolean propagateSelectionRec(Mutable<ILogicalOperator> sigmaRef, Mutable<ILogicalOperator> opRef2)
            throws AlgebricksException {
        AbstractLogicalOperator op2 = (AbstractLogicalOperator) opRef2.getValue();
        if (op2.getInputs().size() != 1 || op2.getOperatorTag() == LogicalOperatorTag.DATASOURCESCAN) {
            return false;
        }

        SelectOperator sigma = (SelectOperator) sigmaRef.getValue();
        LinkedList<LogicalVariable> usedInSigma = new LinkedList<LogicalVariable>();
        sigma.getCondition().getValue().getUsedVariables(usedInSigma);

        LinkedList<LogicalVariable> produced2 = new LinkedList<LogicalVariable>();
        VariableUtilities.getProducedVariables(op2, produced2);
        if (OperatorPropertiesUtil.disjoint(produced2, usedInSigma)) {
            // just swap
            opRef2.setValue(sigma);
            sigmaRef.setValue(op2);
            List<Mutable<ILogicalOperator>> sigmaInpList = sigma.getInputs();
            sigmaInpList.clear();
            sigmaInpList.addAll(op2.getInputs());
            List<Mutable<ILogicalOperator>> op2InpList = op2.getInputs();
            op2InpList.clear();
            op2InpList.add(opRef2);
            propagateSelectionRec(opRef2, sigma.getInputs().get(0));
            return true;

        }
        return false;
    }

}
