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

package org.apache.hyracks.algebricks.core.algebra.operators.physical;

import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.algebra.properties.LocalMemoryRequirements;
import org.apache.hyracks.api.exceptions.ErrorCode;

public abstract class AbstractGroupByPOperator extends AbstractPhysicalOperator {

    // variable memory, min 4 frames
    public static final int MIN_FRAME_LIMIT_FOR_GROUP_BY = 4;

    protected List<LogicalVariable> columnList;

    AbstractGroupByPOperator(List<LogicalVariable> columnList) {
        this.columnList = columnList;
    }

    List<LogicalVariable> getGroupByColumns() {
        return columnList;
    }

    int[] getFdColumns(GroupByOperator gby, IOperatorSchema inputSchema) throws AlgebricksException {
        int numFds = gby.getDecorList().size();
        int fdColumns[] = new int[numFds];
        int j = 0;
        for (Pair<LogicalVariable, Mutable<ILogicalExpression>> p : gby.getDecorList()) {
            ILogicalExpression expr = p.second.getValue();
            if (expr.getExpressionTag() != LogicalExpressionTag.VARIABLE) {
                throw AlgebricksException.create(ErrorCode.EXPR_NOT_NORMALIZED, expr.getSourceLocation());
            }
            VariableReferenceExpression v = (VariableReferenceExpression) expr;
            LogicalVariable decor = v.getVariableReference();
            fdColumns[j++] = inputSchema.findVariable(decor);
        }
        return fdColumns;
    }

    void checkGroupAll(GroupByOperator groupByOp) throws AlgebricksException {
        if (groupByOp.isGroupAll() && !groupByOp.getDecorList().isEmpty()) {
            throw AlgebricksException.create(ErrorCode.GROUP_ALL_DECOR, groupByOp.getSourceLocation());
        }
    }

    public void setGroupByColumns(List<LogicalVariable> columnList) {
        this.columnList = columnList;
    }

    @Override
    public boolean expensiveThanMaterialization() {
        return true;
    }

    @Override
    public void createLocalMemoryRequirements(ILogicalOperator op) {
        localMemoryRequirements = LocalMemoryRequirements.variableMemoryBudget(MIN_FRAME_LIMIT_FOR_GROUP_BY);
    }

    @Override
    public String toString() {
        return getOperatorTag().toString() + columnList;
    }
}