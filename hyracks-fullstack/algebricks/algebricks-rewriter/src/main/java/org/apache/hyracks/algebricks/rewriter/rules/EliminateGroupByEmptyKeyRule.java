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

import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * This rule lift out the aggregate operator out from a group-by operator
 * if the gby operator groups-by on empty key, e.g., the group-by variables are empty.
 *
 * @author yingyib
 */
public class EliminateGroupByEmptyKeyRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
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
        // Only groupAll has equivalent semantics to aggregate.
        if (!groupOp.isGroupAll()) {
            return false;
        }
        List<LogicalVariable> groupVars = groupOp.getGroupByVarList();
        List<Pair<LogicalVariable, Mutable<ILogicalExpression>>> decorList = groupOp.getDecorList();
        if (!groupVars.isEmpty() || !decorList.isEmpty()) {
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
        while (currentOpRef.getValue().getInputs() != null && !currentOpRef.getValue().getInputs().isEmpty()) {
            currentOpRef = currentOpRef.getValue().getInputs().get(0);
        }
        return currentOpRef;
    }

}
