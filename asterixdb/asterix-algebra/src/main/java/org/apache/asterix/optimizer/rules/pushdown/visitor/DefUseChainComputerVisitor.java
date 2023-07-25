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
package org.apache.asterix.optimizer.rules.pushdown.visitor;

import java.util.List;

import org.apache.asterix.optimizer.rules.pushdown.PushdownContext;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionReferenceTransform;

public class DefUseChainComputerVisitor implements ILogicalExpressionReferenceTransform {
    private final PushdownContext pushdownContext;
    private ILogicalOperator operator;
    private List<LogicalVariable> producedVariables;
    private int expressionIndex;

    public DefUseChainComputerVisitor(PushdownContext pushdownContext) {
        this.pushdownContext = pushdownContext;
    }

    public void init(ILogicalOperator operator, List<LogicalVariable> producedVariables) {
        this.operator = operator;
        this.producedVariables = producedVariables;
        expressionIndex = 0;
    }

    @Override
    public boolean transform(Mutable<ILogicalExpression> exprRef) throws AlgebricksException {
        ILogicalExpression expression = exprRef.getValue();
        // compute use chain first
        LogicalVariable producedVariable = producedVariables != null ? producedVariables.get(expressionIndex) : null;
        pushdownContext.use(operator, expression, expressionIndex, producedVariable);
        if (producedVariable != null) {
            // then define the produced variable
            pushdownContext.define(producedVariable, operator, expression, expressionIndex);
        }
        expressionIndex++;
        return false;
    }
}
