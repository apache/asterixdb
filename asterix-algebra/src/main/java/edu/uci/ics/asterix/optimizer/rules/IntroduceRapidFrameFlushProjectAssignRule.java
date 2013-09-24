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

import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.asterix.algebra.operators.CommitOperator;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.ExtensionOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.ProjectOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.physical.AssignPOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.physical.StreamProjectPOperator;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * This rule will search for project and assign operators in an insert/delete/update plan and
 * pass a hint to all of them. This hint is used by the project and assign operators so that frames are pushed to
 * the next operator without waiting until they get full. The purpose of this is to
 * reduce the time of holding exclusive locks on the keys that have been inserted. Also to allow feeds batching
 * to work correctly.
 * 
 * @author salsubaiee
 */
public class IntroduceRapidFrameFlushProjectAssignRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
        return false;
    }

    private boolean checkIfRuleIsApplicable(AbstractLogicalOperator op) {
        if (op.getOperatorTag() != LogicalOperatorTag.EXTENSION_OPERATOR) {
            return false;
        }
        ExtensionOperator extensionOp = (ExtensionOperator) op;
        if (!(extensionOp.getDelegate() instanceof CommitOperator)) {
            return false;
        }

        for (int i = 0; i < op.getInputs().size(); ++i) {
            AbstractLogicalOperator descendantOp = (AbstractLogicalOperator) op.getInputs().get(i).getValue();

            if (descendantOp.getOperatorTag() == LogicalOperatorTag.PROJECT
                    || descendantOp.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
                if (descendantOp.getPhysicalOperator() == null) {
                    return false;
                }
            }
            checkIfRuleIsApplicable(descendantOp);
        }
        return true;
    }

    private boolean changeRule(AbstractLogicalOperator op) {
        boolean planModified = false;
        for (int i = 0; i < op.getInputs().size(); ++i) {
            AbstractLogicalOperator descendantOp = (AbstractLogicalOperator) op.getInputs().get(i).getValue();

            if (descendantOp.getOperatorTag() == LogicalOperatorTag.PROJECT) {
                ProjectOperator projectOp = (ProjectOperator) descendantOp;
                StreamProjectPOperator physicalOp = (StreamProjectPOperator) projectOp.getPhysicalOperator();
                physicalOp.setRapidFrameFlush(true);
                planModified = true;
            } else if (descendantOp.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
                AssignOperator assignOp = (AssignOperator) descendantOp;
                AssignPOperator physicalOp = (AssignPOperator) assignOp.getPhysicalOperator();
                physicalOp.setRapidFrameFlush(true);
                planModified = true;
            }
            changeRule(descendantOp);
        }
        return planModified;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {

        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (!checkIfRuleIsApplicable(op)) {
            return false;
        }
        return changeRule(op);
    }
}