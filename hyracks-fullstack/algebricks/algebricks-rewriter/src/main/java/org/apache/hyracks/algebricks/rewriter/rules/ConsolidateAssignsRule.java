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

import java.util.HashSet;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class ConsolidateAssignsRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.ASSIGN) {
            return false;
        }
        AssignOperator assign1 = (AssignOperator) op;

        AbstractLogicalOperator op2 = (AbstractLogicalOperator) assign1.getInputs().get(0).getValue();
        if (op2.getOperatorTag() != LogicalOperatorTag.ASSIGN) {
            return false;
        }

        if (!OperatorPropertiesUtil.isMovable(op) || !OperatorPropertiesUtil.isMovable(op2)) {
            return false;
        }

        AssignOperator assign2 = (AssignOperator) op2;

        HashSet<LogicalVariable> used1 = new HashSet<LogicalVariable>();
        VariableUtilities.getUsedVariables(assign1, used1);
        for (LogicalVariable v2 : assign2.getVariables()) {
            if (used1.contains(v2)) {
                return false;
            }
        }

        assign1.getVariables().addAll(assign2.getVariables());
        assign1.getExpressions().addAll(assign2.getExpressions());

        Mutable<ILogicalOperator> botOpRef = assign2.getInputs().get(0);
        List<Mutable<ILogicalOperator>> asgnInpList = assign1.getInputs();
        asgnInpList.clear();
        asgnInpList.add(botOpRef);
        context.computeAndSetTypeEnvironmentForOperator(assign1);
        return true;
    }

}
