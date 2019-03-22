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
package org.apache.asterix.optimizer.rules;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InnerJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class PushProperJoinThroughProduct implements IAlgebraicRewriteRule {

    private List<LogicalVariable> usedInCond1AndMaps = new ArrayList<LogicalVariable>();
    private List<LogicalVariable> productLeftBranchVars = new ArrayList<LogicalVariable>();

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        LogicalOperatorTag tag1 = op.getOperatorTag();
        if (tag1 != LogicalOperatorTag.INNERJOIN && tag1 != LogicalOperatorTag.LEFTOUTERJOIN) {
            return false;
        }
        AbstractBinaryJoinOperator join1 = (AbstractBinaryJoinOperator) op;
        ILogicalExpression cond1 = join1.getCondition().getValue();
        // don't try to push a product down
        if (OperatorPropertiesUtil.isAlwaysTrueCond(cond1)) {
            return false;
        }

        Mutable<ILogicalOperator> opRef2 = op.getInputs().get(0);

        AbstractLogicalOperator op2 = (AbstractLogicalOperator) opRef2.getValue();
        while (op2.isMap()) {
            opRef2 = op2.getInputs().get(0);
            op2 = (AbstractLogicalOperator) opRef2.getValue();
        }

        if (op2.getOperatorTag() != LogicalOperatorTag.INNERJOIN) {
            return false;
        }

        InnerJoinOperator product = (InnerJoinOperator) op2;
        if (!OperatorPropertiesUtil.isAlwaysTrueCond(product.getCondition().getValue())) {
            return false;
        }

        usedInCond1AndMaps.clear();
        cond1.getUsedVariables(usedInCond1AndMaps);
        Mutable<ILogicalOperator> opIterRef = op.getInputs().get(0);
        ILogicalOperator opIter = opIterRef.getValue();
        do {
            VariableUtilities.getUsedVariables(opIter, usedInCond1AndMaps);
            opIterRef = opIter.getInputs().get(0);
            opIter = opIterRef.getValue();
        } while (opIter.isMap());

        productLeftBranchVars.clear();
        ILogicalOperator opLeft = op2.getInputs().get(0).getValue();
        VariableUtilities.getLiveVariables(opLeft, productLeftBranchVars);

        if (!OperatorPropertiesUtil.disjoint(usedInCond1AndMaps, productLeftBranchVars)) {
            return false;
        }

        // now push the operators from in between joins, too
        opIterRef = op.getInputs().get(0);
        opIter = opIterRef.getValue();

        Mutable<ILogicalOperator> op3Ref = product.getInputs().get(1);
        ILogicalOperator op3 = op3Ref.getValue();

        opRef2.setValue(op3);
        op3Ref.setValue(join1);
        opRef.setValue(product);
        return true;
    }
}
