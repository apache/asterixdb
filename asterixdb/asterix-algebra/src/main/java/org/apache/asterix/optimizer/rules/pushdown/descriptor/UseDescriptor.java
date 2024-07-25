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
package org.apache.asterix.optimizer.rules.pushdown.descriptor;

import java.util.HashSet;
import java.util.Set;

import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;

public class UseDescriptor extends AbstractDescriptor {
    private final Set<LogicalVariable> usedVariables;
    private final LogicalVariable producedVariable;

    public UseDescriptor(int scope, ILogicalOperator subplanOperator, ILogicalOperator operator,
            ILogicalExpression expression, int expressionIndex, LogicalVariable producedVariable) {
        super(scope, subplanOperator, operator, expression, expressionIndex);
        this.usedVariables = new HashSet<>();
        this.producedVariable = producedVariable;
    }

    public Set<LogicalVariable> getUsedVariables() {
        return usedVariables;
    }

    public LogicalVariable getProducedVariable() {
        return producedVariable;
    }

    @Override
    public String toString() {
        return operator.getOperatorTag() + ": [" + expression + "]";
    }
}
