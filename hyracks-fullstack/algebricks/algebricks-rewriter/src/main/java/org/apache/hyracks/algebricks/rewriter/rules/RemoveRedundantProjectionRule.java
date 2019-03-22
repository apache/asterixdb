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
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ProjectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/*
 *  project [var-list1]
 *   project [var-list2]
 *     P
 *
 *  if var-list1.equals(var-list2) becomes
 *
 *  project [var-list1]
 *    P
 *
 */

public class RemoveRedundantProjectionRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        return false;
    }

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op1 = (AbstractLogicalOperator) opRef.getValue();
        if (op1.getOperatorTag() == LogicalOperatorTag.PROJECT) {
            Mutable<ILogicalOperator> opRef2 = op1.getInputs().get(0);
            AbstractLogicalOperator op2 = (AbstractLogicalOperator) opRef2.getValue();
            if (op2.getOperatorTag() != LogicalOperatorTag.PROJECT) {
                return false;
            }
            ProjectOperator pi2 = (ProjectOperator) op2;
            opRef2.setValue(pi2.getInputs().get(0).getValue());
        } else {
            if (op1.getInputs().size() <= 0)
                return false;
            Mutable<ILogicalOperator> opRef2 = op1.getInputs().get(0);
            AbstractLogicalOperator op2 = (AbstractLogicalOperator) opRef2.getValue();
            if (op2.getOperatorTag() != LogicalOperatorTag.PROJECT) {
                return false;
            }
            if (op2.getInputs().size() <= 0)
                return false;
            Mutable<ILogicalOperator> opRef3 = op2.getInputs().get(0);
            AbstractLogicalOperator op3 = (AbstractLogicalOperator) opRef3.getValue();

            List<LogicalVariable> liveVars2 = new ArrayList<LogicalVariable>();
            List<LogicalVariable> liveVars3 = new ArrayList<LogicalVariable>();

            VariableUtilities.getLiveVariables(op2, liveVars2);
            VariableUtilities.getLiveVariables(op3, liveVars3);

            if (!VariableUtilities.varListEqualUnordered(liveVars2, liveVars3))
                return false;
            opRef2.setValue(op3);
        }

        return true;
    }

}
