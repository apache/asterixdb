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

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.asterix.optimizer.rules.pushdown.schema.AbstractComplexExpectedSchemaNode;
import org.apache.asterix.optimizer.rules.pushdown.schema.AnyExpectedSchemaNode;
import org.apache.asterix.optimizer.rules.pushdown.schema.ArrayExpectedSchemaNode;
import org.apache.asterix.optimizer.rules.pushdown.schema.ExpectedSchemaNodeType;
import org.apache.asterix.optimizer.rules.pushdown.schema.IExpectedSchemaNode;
import org.apache.asterix.optimizer.rules.pushdown.schema.IExpectedSchemaNodeVisitor;
import org.apache.asterix.optimizer.rules.pushdown.schema.ObjectExpectedSchemaNode;
import org.apache.asterix.optimizer.rules.pushdown.schema.RootExpectedSchemaNode;
import org.apache.asterix.optimizer.rules.pushdown.schema.UnionExpectedSchemaNode;

public class ExpectedSchemaMergerVisitor
        implements IExpectedSchemaNodeVisitor<IExpectedSchemaNode, IExpectedSchemaNode> {
    private AbstractComplexExpectedSchemaNode currentParent;

    public RootExpectedSchemaNode merge(RootExpectedSchemaNode left, RootExpectedSchemaNode right) {
        if (left == null) {
            return right;
        } else if (right == null) {
            return left;
        }

        return visit(left, right);
    }

    @Override
    public RootExpectedSchemaNode visit(RootExpectedSchemaNode node, IExpectedSchemaNode arg) {
        if (!(arg instanceof RootExpectedSchemaNode)) {
            // Safeguard against merging root with non-root
            throw new IllegalStateException("Cannot merge root with non-root node");
        }

        // The other must be root
        RootExpectedSchemaNode argRoot = (RootExpectedSchemaNode) arg;
        if (node.isAllFields() || argRoot.isAllFields()) {
            // if either of the two roots require the entire fields, then return all fields for both
            return RootExpectedSchemaNode.ALL_FIELDS_ROOT_IRREPLACEABLE_NODE;
        }

        // if either the root or argRoot is empty, then return the non-empty node
        if (argRoot.isEmpty()) {
            return node;
        } else if (node.isEmpty()) {
            return argRoot;
        }

        // combine
        RootExpectedSchemaNode mergedRoot = (RootExpectedSchemaNode) RootExpectedSchemaNode.ALL_FIELDS_ROOT_NODE
                .replaceIfNeeded(ExpectedSchemaNodeType.OBJECT, node.getSourceLocation(), node.getFunctionName());
        mergeObjectFields(mergedRoot, node.getChildren(), argRoot.getChildren());
        return mergedRoot;
    }

    @Override
    public IExpectedSchemaNode visit(ObjectExpectedSchemaNode node, IExpectedSchemaNode arg) {
        ObjectExpectedSchemaNode mergedObject =
                new ObjectExpectedSchemaNode(currentParent, node.getSourceLocation(), node.getFunctionName());
        Map<String, IExpectedSchemaNode> argChildren = Collections.emptyMap();
        if (arg != null) {
            ObjectExpectedSchemaNode argObject = (ObjectExpectedSchemaNode) arg;
            argChildren = argObject.getChildren();
        }

        mergeObjectFields(mergedObject, node.getChildren(), argChildren);

        return mergedObject;
    }

    @Override
    public IExpectedSchemaNode visit(ArrayExpectedSchemaNode node, IExpectedSchemaNode arg) {
        IExpectedSchemaNode nodeItem = node.getChild();
        IExpectedSchemaNode argItem = null;
        if (arg != null) {
            ArrayExpectedSchemaNode arrayArg = (ArrayExpectedSchemaNode) arg;
            argItem = arrayArg.getChild();
        }
        ArrayExpectedSchemaNode mergedArray =
                new ArrayExpectedSchemaNode(currentParent, node.getSourceLocation(), node.getFunctionName());
        AbstractComplexExpectedSchemaNode previousParent = currentParent;
        currentParent = mergedArray;
        IExpectedSchemaNode mergedItem = merge(nodeItem, argItem);
        mergedArray.addChild(mergedItem);
        currentParent = previousParent;

        return mergedArray;
    }

    @Override
    public IExpectedSchemaNode visit(UnionExpectedSchemaNode node, IExpectedSchemaNode arg) {
        UnionExpectedSchemaNode union =
                new UnionExpectedSchemaNode(currentParent, node.getSourceLocation(), node.getFunctionName());
        AbstractComplexExpectedSchemaNode previousParent = currentParent;
        currentParent = union;

        if (arg == null) {
            // arg is null, make a copy of the current union children
            for (Map.Entry<ExpectedSchemaNodeType, AbstractComplexExpectedSchemaNode> nodeChild : node.getChildren()) {
                IExpectedSchemaNode mergedChild = merge(nodeChild.getValue(), null);
                union.addChild((AbstractComplexExpectedSchemaNode) mergedChild);
            }
        } else if (arg.getType() == ExpectedSchemaNodeType.UNION) {
            // Merging two unions
            UnionExpectedSchemaNode argUnion = (UnionExpectedSchemaNode) arg;
            for (Map.Entry<ExpectedSchemaNodeType, AbstractComplexExpectedSchemaNode> nodeChild : node.getChildren()) {
                IExpectedSchemaNode child = nodeChild.getValue();
                IExpectedSchemaNode argChild = argUnion.getChild(child.getType());
                IExpectedSchemaNode mergedChild = merge(child, argChild);
                union.addChild((AbstractComplexExpectedSchemaNode) mergedChild);
            }
        } else {
            // Merging a union with either an array or an object
            for (Map.Entry<ExpectedSchemaNodeType, AbstractComplexExpectedSchemaNode> nodeChild : node.getChildren()) {
                IExpectedSchemaNode child = nodeChild.getValue();
                IExpectedSchemaNode argNode = child.getType() == arg.getType() ? arg : null;
                IExpectedSchemaNode mergedChild = merge(child, argNode);
                union.addChild((AbstractComplexExpectedSchemaNode) mergedChild);
            }
        }

        currentParent = previousParent;

        return union;
    }

    @Override
    public IExpectedSchemaNode visit(AnyExpectedSchemaNode node, IExpectedSchemaNode arg) {
        return new AnyExpectedSchemaNode(currentParent, node.getSourceLocation(), node.getFunctionName());
    }

    private void mergeObjectFields(ObjectExpectedSchemaNode objectNode, Map<String, IExpectedSchemaNode> left,
            Map<String, IExpectedSchemaNode> right) {
        AbstractComplexExpectedSchemaNode previousParent = currentParent;
        currentParent = objectNode;
        Set<String> mergedFields = new HashSet<>();
        mergeObjectFields(objectNode, left, right, mergedFields);
        mergeObjectFields(objectNode, right, left, mergedFields);
        currentParent = previousParent;
    }

    private void mergeObjectFields(ObjectExpectedSchemaNode objectNode, Map<String, IExpectedSchemaNode> left,
            Map<String, IExpectedSchemaNode> right, Set<String> mergedFields) {
        for (Map.Entry<String, IExpectedSchemaNode> leftChild : left.entrySet()) {
            String fieldName = leftChild.getKey();
            if (mergedFields.contains(fieldName)) {
                continue;
            }
            IExpectedSchemaNode rightChild = right.get(fieldName);
            IExpectedSchemaNode mergedChild = merge(leftChild.getValue(), rightChild);
            objectNode.addChild(fieldName, mergedChild);
            mergedFields.add(fieldName);
        }
    }

    private IExpectedSchemaNode merge(IExpectedSchemaNode leftChild, IExpectedSchemaNode rightChild) {
        if (rightChild == null || leftChild.getType() == ExpectedSchemaNodeType.ANY
                || leftChild.getType() == ExpectedSchemaNodeType.UNION) {
            // if rightChild is null then visit leftChild to create a new copy of it
            // else if leftChild is ANY then return ANY (as everything is required from this child)
            // else if leftChild is UNION, then visit left first
            return leftChild.accept(this, rightChild);
        } else if (rightChild.getType() == ExpectedSchemaNodeType.ANY
                || rightChild.getType() == ExpectedSchemaNodeType.UNION) {
            // if rightChild is ANY then return ANY (as everything is required from this child)
            // else if rightChild is UNION, then visit right first
            return rightChild.accept(this, leftChild);
        } else if (leftChild.getType() != rightChild.getType()) {
            // leftChild and rightChild are not the same and both are either object or array
            return createUnionNode(leftChild, rightChild);
        }

        return leftChild.accept(this, rightChild);
    }

    private IExpectedSchemaNode createUnionNode(IExpectedSchemaNode leftChild, IExpectedSchemaNode rightChild) {
        UnionExpectedSchemaNode union =
                new UnionExpectedSchemaNode(currentParent, leftChild.getSourceLocation(), leftChild.getFunctionName());
        AbstractComplexExpectedSchemaNode previousParent = currentParent;
        currentParent = union;
        // Create a copy of leftChild
        IExpectedSchemaNode leftNewChild = leftChild.accept(this, null);
        // Create a copy of rightChild
        IExpectedSchemaNode rightNewChild = rightChild.accept(this, null);
        // Add both left and right children to the union node
        union.addChild((AbstractComplexExpectedSchemaNode) leftNewChild);
        union.addChild((AbstractComplexExpectedSchemaNode) rightNewChild);
        currentParent = previousParent;
        return union;
    }
}
