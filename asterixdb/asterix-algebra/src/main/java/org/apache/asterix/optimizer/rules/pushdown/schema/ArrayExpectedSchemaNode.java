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

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;

public class ArrayExpectedSchemaNode extends AbstractComplexExpectedSchemaNode {
    private IExpectedSchemaNode child;

    public ArrayExpectedSchemaNode(AbstractComplexExpectedSchemaNode parent,
            AbstractFunctionCallExpression parentExpression, ILogicalExpression expression) {
        super(parent, parentExpression, expression);
    }

    @Override
    public ExpectedSchemaNodeType getType() {
        return ExpectedSchemaNodeType.ARRAY;
    }

    public IExpectedSchemaNode getChild() {
        return child;
    }

    public void addChild(IExpectedSchemaNode child) {
        this.child = child;
    }

    @Override
    public <R, T> R accept(IExpectedSchemaNodeVisitor<R, T> visitor, T arg) {
        return visitor.visit(this, arg);
    }

    @Override
    public IExpectedSchemaNode replaceChild(IExpectedSchemaNode oldNode, IExpectedSchemaNode newNode) {
        if (child.getType() == newNode.getType() || isReplaceableAny(newNode)) {
            // We are trying to replace with the same node type, or with a replaceable any, ignore.
            return child;
        } else if (isChildReplaceable(child, newNode)) {
            child = newNode;
            return child;
        }

        // This should never happen, but safeguard against unexpected behavior
        throw new IllegalStateException("Cannot replace " + child.getType() + " with " + newNode.getType());
    }

    @Override
    protected IExpectedSchemaNode getChildNode(AbstractFunctionCallExpression parentExpr) throws AlgebricksException {
        return child;
    }
}
