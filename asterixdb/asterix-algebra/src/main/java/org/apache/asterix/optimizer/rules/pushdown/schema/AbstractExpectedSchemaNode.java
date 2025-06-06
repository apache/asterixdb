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
package org.apache.asterix.optimizer.rules.pushdown.schema;

import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.api.exceptions.SourceLocation;

abstract class AbstractExpectedSchemaNode implements IExpectedSchemaNode {
    protected final AbstractFunctionCallExpression parentExpression;
    protected final ILogicalExpression expression;
    private AbstractComplexExpectedSchemaNode parent;

    AbstractExpectedSchemaNode(AbstractComplexExpectedSchemaNode parent,
            AbstractFunctionCallExpression parentExpression, ILogicalExpression expression) {
        this.parentExpression = parentExpression;
        this.expression = expression;
        this.parent = parent;
    }

    @Override
    public final AbstractComplexExpectedSchemaNode getParent() {
        return parent;
    }

    @Override
    public final SourceLocation getSourceLocation() {
        return expression.getSourceLocation();
    }

    @Override
    public final String getFunctionName() {
        if (expression.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
            return ((AbstractFunctionCallExpression) expression).getFunctionIdentifier().getName();
        }
        return null;
    }

    @Override
    public AbstractFunctionCallExpression getParentExpression() {
        return parentExpression;
    }

    @Override
    public ILogicalExpression getExpression() {
        return expression;
    }

    @Override
    public void setParent(AbstractComplexExpectedSchemaNode parent) {
        this.parent = parent;
    }
}
