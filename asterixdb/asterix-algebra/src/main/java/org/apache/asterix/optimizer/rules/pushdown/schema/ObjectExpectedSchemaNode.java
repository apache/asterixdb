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

import java.util.HashMap;
import java.util.Map;

import org.apache.asterix.metadata.utils.PushdownUtil;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

public class ObjectExpectedSchemaNode extends AbstractComplexExpectedSchemaNode {
    private final Map<String, IExpectedSchemaNode> children;
    private final Int2ObjectMap<String> fieldIdToFieldName;

    public ObjectExpectedSchemaNode(AbstractComplexExpectedSchemaNode parent,
            AbstractFunctionCallExpression parentExpression, ILogicalExpression expression) {
        super(parent, parentExpression, expression);
        children = new HashMap<>();
        fieldIdToFieldName = new Int2ObjectOpenHashMap<>();
    }

    public boolean isRoot() {
        return false;
    }

    public void addChild(String fieldName, int fieldId, IExpectedSchemaNode child) {
        FunctionIdentifier fid = child.getParentExpression().getFunctionIdentifier();
        children.put(fieldName, child);
        if (fieldId > -1) {
            fieldIdToFieldName.put(fieldId, fieldName);
        }
    }

    public void addAllFieldNameIds(ObjectExpectedSchemaNode node) {
        fieldIdToFieldName.putAll(node.fieldIdToFieldName);
    }

    public Map<String, IExpectedSchemaNode> getChildren() {
        return children;
    }

    @Override
    public ExpectedSchemaNodeType getType() {
        return ExpectedSchemaNodeType.OBJECT;
    }

    @Override
    public <R, T> R accept(IExpectedSchemaNodeVisitor<R, T> visitor, T arg) {
        return visitor.visit(this, arg);
    }

    @Override
    public IExpectedSchemaNode replaceChild(IExpectedSchemaNode oldNode, IExpectedSchemaNode newNode) {
        String fieldName = getChildFieldName(oldNode);
        IExpectedSchemaNode child = children.get(fieldName);
        if (child.getType() == newNode.getType() || isReplaceableAny(newNode)) {
            // We are trying to replace with the same node type, or with a replaceable any, ignore.
            return child;
        } else if (isChildReplaceable(child, newNode)) {
            children.replace(fieldName, newNode);
            return newNode;
        }

        // This should never happen, but safeguard against unexpected behavior
        throw new IllegalStateException("Cannot replace " + child.getType() + " with " + newNode.getType());
    }

    public String getChildFieldName(IExpectedSchemaNode requestedChild) {
        AbstractFunctionCallExpression expr = requestedChild.getParentExpression();
        int fieldNameId = PushdownUtil.getFieldNameId(requestedChild.getParentExpression());

        if (fieldNameId > -1) {
            return fieldIdToFieldName.get(fieldNameId);
        }

        return PushdownUtil.getFieldName(expr);
    }
}
