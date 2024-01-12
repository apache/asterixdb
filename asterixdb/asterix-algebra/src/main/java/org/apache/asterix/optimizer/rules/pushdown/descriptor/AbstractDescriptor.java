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

import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;

class AbstractDescriptor {
    protected final int scope;
    protected final ILogicalOperator subplanOperator;
    protected final ILogicalOperator operator;
    protected final ILogicalExpression expression;
    protected final int expressionIndex;

    public AbstractDescriptor(int scope, ILogicalOperator subplanOperator, ILogicalOperator operator,
            ILogicalExpression expression, int expressionIndex) {
        this.scope = scope;
        this.subplanOperator = subplanOperator;
        this.operator = operator;
        this.expression = expression;
        this.expressionIndex = expressionIndex;
    }

    public ILogicalOperator getOperator() {
        return operator;
    }

    public ILogicalExpression getExpression() {
        return expression;
    }

    public int getExpressionIndex() {
        return expressionIndex;
    }

    public int getScope() {
        return scope;
    }

    public boolean inSubplan() {
        return subplanOperator != null;
    }

    public ILogicalOperator getSubplanOperator() {
        return subplanOperator;
    }
}
