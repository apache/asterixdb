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

import static org.apache.asterix.optimizer.rules.pushdown.schema.ExpectedSchemaBuilder.getExpectedNestedNodeType;

import java.util.EnumMap;
import java.util.Map;
import java.util.Set;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;

public class UnionExpectedSchemaNode extends AbstractComplexExpectedSchemaNode {
    private final Map<ExpectedSchemaNodeType, AbstractComplexExpectedSchemaNode> children;

    public UnionExpectedSchemaNode(AbstractComplexExpectedSchemaNode parent,
            AbstractFunctionCallExpression expression) {
        super(parent, expression, expression);
        children = new EnumMap<>(ExpectedSchemaNodeType.class);
    }

    /**
     * A UNION type must have both ARRAY and OBJECT when first created - the only possible values. Thus, we cannot
     * replace a child of a UNION type to ANY. We can only replace the union itself to ANY.
     */
    @Override
    protected IExpectedSchemaNode replaceChild(IExpectedSchemaNode oldChildNode, IExpectedSchemaNode newChildNode) {
        throw new UnsupportedOperationException("Cannot replace a child of UNION");
    }

    public void addChild(AbstractComplexExpectedSchemaNode node) {
        children.put(node.getType(), node);
    }

    public void createChild(ExpectedSchemaNodeType nodeType, AbstractFunctionCallExpression parentExpression,
            ILogicalExpression expression) {
        if (parentExpression == null) {
            // Should never happen
            throw new NullPointerException("expression is null");
        }
        children.computeIfAbsent(nodeType, k -> createNestedNode(k, this, parentExpression, expression));
    }

    public AbstractComplexExpectedSchemaNode getChild(ExpectedSchemaNodeType type) {
        return children.get(type);
    }

    public Set<Map.Entry<ExpectedSchemaNodeType, AbstractComplexExpectedSchemaNode>> getChildren() {
        return children.entrySet();
    }

    @Override
    public ExpectedSchemaNodeType getType() {
        return ExpectedSchemaNodeType.UNION;
    }

    @Override
    public <R, T> R accept(IExpectedSchemaNodeVisitor<R, T> visitor, T arg) {
        return visitor.visit(this, arg);
    }

    /**
     * We override this method to handle heterogeneous values while UNION exists. We do not need to create another
     * UNION type - we simply return this. In case we want to fallback to ANY node, we call the super method.
     *
     * @param expectedNodeType the expected type
     * @param parentExpression
     * @param expression
     * @return ANY or this
     */
    @Override
    public IExpectedSchemaNode replaceIfNeeded(ExpectedSchemaNodeType expectedNodeType,
            AbstractFunctionCallExpression parentExpression, ILogicalExpression expression) {
        if (expectedNodeType == ExpectedSchemaNodeType.ANY) {
            return super.replaceIfNeeded(expectedNodeType, parentExpression, expression);
        }
        return this;
    }

    @Override
    protected IExpectedSchemaNode getChildNode(AbstractFunctionCallExpression parentExpr) throws AlgebricksException {
        ExpectedSchemaNodeType parentType = getExpectedNestedNodeType(parentExpr);
        AbstractComplexExpectedSchemaNode actualParent = getChild(parentType);
        return actualParent.getChildNode(parentExpr);
    }
}
