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
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;

public abstract class AbstractComplexExpectedSchemaNode extends AbstractExpectedSchemaNode {

    AbstractComplexExpectedSchemaNode(AbstractComplexExpectedSchemaNode parent,
            AbstractFunctionCallExpression parentExpression, ILogicalExpression expression) {
        super(parent, parentExpression, expression);
    }

    @Override
    public boolean allowsReplacing() {
        return true;
    }

    @Override
    public IExpectedSchemaNode replaceIfNeeded(ExpectedSchemaNodeType expectedNodeType,
            AbstractFunctionCallExpression parentExpression, ILogicalExpression expression) {
        //If no change is required, return the same node
        IExpectedSchemaNode node = this;
        if (expectedNodeType == ExpectedSchemaNodeType.ANY) {
            /*
             * We want to fall back to ANY. This could happen if we needed one nested value in one expression but
             * another expression, the entire node is needed. So, we fall back to ANY and remove any information
             * about the nested value. For example:
             * SELECT t.hashtags[*].text, t.hashtags
             * FROM Tweets t
             * In this case, we first saw (t.hashtags[*].text), but the next expression (t.hashtags) requested
             * the entire hashtags. So, the expected type for hashtags should be ANY
             */
            node = new AnyExpectedSchemaNode(getParent(), getParentExpression(), false);
            getParent().replaceChild(this, node);
        } else if (expectedNodeType != getType()) {
            /*
             * We need to change the type to UNION, as the same value was accessed as an ARRAY and as an OBJECT.
             * This is possible if we have heterogeneous value access in the query.
             */

            //Create UNION node and its parent is the parent of this
            UnionExpectedSchemaNode unionSchemaNode = new UnionExpectedSchemaNode(getParent(), getParentExpression());

            //Add this as a child of UNION
            unionSchemaNode.addChild(this);
            /*
             * Replace the reference of this in its parent with the union node
             * Before: parent --> this
             * After:  parent --> UNION --> this
             */
            getParent().replaceChild(this, unionSchemaNode);
            /*
             * Set the parent of this to union
             * Before: oldParent <-- this
             * After:  oldParent <-- UNION <-- this
             */
            setParent(unionSchemaNode);
            /*
             * Add the new child with the expected type to union
             * Before: UNION <-- this
             * After:  UNION <-- (this, newChild)
             */
            unionSchemaNode.createChild(expectedNodeType, parentExpression, expression);
            node = unionSchemaNode;
        }
        return node;
    }

    protected abstract IExpectedSchemaNode replaceChild(IExpectedSchemaNode oldNode, IExpectedSchemaNode newNode);

    /**
     * A child is replaceable if
     * - child is allowed to be replaced
     * - AND either of the following is satisfied:
     * - - child is of type {@link ExpectedSchemaNodeType#ANY}
     * - - OR the newNode is of type {@link ExpectedSchemaNodeType#UNION}
     * - - OR the newNode is not replaceable
     *
     * @param child   current child
     * @param newNode the new node to replace the current child
     * @return true if child is replaceable, false otherwise
     */
    protected boolean isChildReplaceable(IExpectedSchemaNode child, IExpectedSchemaNode newNode) {
        ExpectedSchemaNodeType childType = child.getType();
        ExpectedSchemaNodeType newType = newNode.getType();
        return child.allowsReplacing() && (childType == ExpectedSchemaNodeType.ANY
                || newType == ExpectedSchemaNodeType.UNION || !newNode.allowsReplacing());
    }

    /**
     * @return true if {@code newNode} is a replaceable {@link ExpectedSchemaNodeType#ANY} node, false otherwise
     */
    protected boolean isReplaceableAny(IExpectedSchemaNode newNode) {
        return newNode.getType() == ExpectedSchemaNodeType.ANY && newNode.allowsReplacing();
    }

    public static AbstractComplexExpectedSchemaNode createNestedNode(ExpectedSchemaNodeType type,
            AbstractComplexExpectedSchemaNode parent, AbstractFunctionCallExpression parentExpression,
            ILogicalExpression myExpr) {
        switch (type) {
            case ARRAY:
                return new ArrayExpectedSchemaNode(parent, parentExpression, myExpr);
            case OBJECT:
                return new ObjectExpectedSchemaNode(parent, parentExpression, myExpr);
            case UNION:
                return new UnionExpectedSchemaNode(parent, parentExpression);
            default:
                throw new IllegalStateException(type + " is not nested or unknown");
        }
    }
}
