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
package org.apache.hyracks.algebricks.rewriter.rules;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class PushMapOperatorDownThroughProductRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op1 = (AbstractLogicalOperator) opRef.getValue();
        // Even the LIMIT operator is a map operator, we don't push LIMIT operator into a join
        // since a new LIMIT under a join can't generate the original result.
        if (!op1.isMap() || op1.getOperatorTag() == LogicalOperatorTag.LIMIT) {
            return false;
        }

        if (!OperatorPropertiesUtil.isMovable(op1)) {
            return false;
        } ;

        Mutable<ILogicalOperator> op2Ref = op1.getInputs().get(0);
        AbstractLogicalOperator op2 = (AbstractLogicalOperator) op2Ref.getValue();
        if (op2.getOperatorTag() != LogicalOperatorTag.INNERJOIN) {
            return false;
        }
        AbstractBinaryJoinOperator join = (AbstractBinaryJoinOperator) op2;
        if (!OperatorPropertiesUtil.isAlwaysTrueCond(join.getCondition().getValue())) {
            return false;
        }

        List<LogicalVariable> used = new ArrayList<LogicalVariable>();
        VariableUtilities.getUsedVariables(op1, used);

        Mutable<ILogicalOperator> b0Ref = op2.getInputs().get(0);
        ILogicalOperator b0 = b0Ref.getValue();
        List<LogicalVariable> b0Scm = new ArrayList<LogicalVariable>();
        VariableUtilities.getLiveVariables(b0, b0Scm);
        if (b0Scm.containsAll(used)) {
            // push operator on left branch
            op2Ref.setValue(b0);
            b0Ref.setValue(op1);
            opRef.setValue(op2);
            return true;
        } else {
            Mutable<ILogicalOperator> b1Ref = op2.getInputs().get(1);
            ILogicalOperator b1 = b1Ref.getValue();
            List<LogicalVariable> b1Scm = new ArrayList<LogicalVariable>();
            VariableUtilities.getLiveVariables(b1, b1Scm);
            if (b1Scm.containsAll(used)) {
                // push operator on right branch
                op2Ref.setValue(b1);
                b1Ref.setValue(op1);
                opRef.setValue(op2);
                return true;
            } else {
                return false;
            }
        }
    }

}
