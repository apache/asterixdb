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

package org.apache.asterix.lang.sqlpp.parser;

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.Literal;
import org.apache.asterix.lang.common.expression.FieldAccessor;
import org.apache.asterix.lang.common.expression.FieldBinding;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.expression.RecordConstructor;
import org.apache.asterix.lang.common.literal.NullLiteral;
import org.apache.asterix.lang.common.literal.StringLiteral;
import org.apache.hyracks.algebricks.common.utils.Pair;

/**
 * Organizes field assignments from SET clauses into a tree structure.
 * This class takes a flat list of field assignments (e.g., u.name = "John", u.age = 30, u.address.city = "NYC")
 * and organizes them into a hierarchical tree where each field path becomes a node. For example, u.address.city
 * creates: root → "address" → "city" with value "NYC".
 * The tree serves two main purposes:
 *   Conflict detection: Prevents setting both a field and its nested child in the same SET clause.
 *      For example, setting both u.name and u.name.first would conflict because a node cannot have both
 *      a value and children.
 *      Record constructor creation: Builds two RecordConstructor expressions by walking the tree:
 *                Transformation record: contains all fields with normal values (for updates/adds)
 *                Deletion record: contains fields set to MISSING (for removal)
 * Example:
 * SET u.name = "John", u.age = 30, u.address.city = "NYC", u.tempField = MISSING
 * Creates a tree structure:
 *   root
 *   ├── name → "John"
 *   ├── age → 30
 *   ├── address
 *   │   └── city → "NYC"
 *   └── tempField → MISSING
 **/
public class SetExpressionTree {
    private Node root;

    public SetExpressionTree() {
        root = new Node("init", null);
    }

    public void insertPath(Expression path, Expression valueExpr) throws CompilationException {
        if (path.getKind() == Expression.Kind.VARIABLE_EXPRESSION) {
            if (root.hasExpression() || root.hasChildren()) {
                throw new CompilationException(ErrorCode.UPDATE_ATTEMPT_ON_CONFLICTING_PATHS, path.getSourceLocation());
            }
            root.setExpression(valueExpr);
            return;
        }
        if (path.getKind() == Expression.Kind.FIELD_ACCESSOR_EXPRESSION) {
            Node candidateNode = accessOrCreatePath((FieldAccessor) path, root);
            if (candidateNode.hasExpression() || candidateNode.hasChildren()) {
                throw new CompilationException(ErrorCode.UPDATE_ATTEMPT_ON_CONFLICTING_PATHS, path.getSourceLocation());
            }
            candidateNode.setExpression(valueExpr);
        }
    }

    public boolean isEmpty() {
        return !root.hasExpression() && !root.hasChildren();
    }

    public Pair<Expression, Expression> createRecordConstructor() {
        return createRecordConstructorInner(root);
    }

    public void resetExpressionTree() {
        root = new Node("init", null);
    }

    private Node accessOrCreatePath(FieldAccessor path, Node node) throws CompilationException {
        Expression leadingExpr = path.getExpr();

        if (leadingExpr.getKind() == Expression.Kind.FIELD_ACCESSOR_EXPRESSION) {
            node = accessOrCreatePath((FieldAccessor) leadingExpr, node);
        }
        if (node.hasExpression()) {
            throw new CompilationException(ErrorCode.UPDATE_ATTEMPT_ON_CONFLICTING_PATHS, path.getSourceLocation());
        }
        return node.retrieveChild(path.getIdent().getValue(), true);
    }

    private Pair<Expression, Expression> createRecordConstructorInner(Node node) {
        if (node.hasExpression()) {
            Expression expr = node.getExpression();
            if (expr.getKind() != Expression.Kind.LITERAL_EXPRESSION) {
                return new Pair<>(expr, null);
            }
            LiteralExpr literalExpr = (LiteralExpr) expr;
            Literal.Type type = literalExpr.getValue().getLiteralType();
            if (type != Literal.Type.MISSING) {
                return new Pair<>(expr, null);
            } else {
                return new Pair<>(null, new LiteralExpr(NullLiteral.INSTANCE));
            }
        }
        List<FieldBinding> setRecordArgs = new ArrayList<>();
        List<FieldBinding> deletionRecordArgs = new ArrayList<>();
        for (Node child : node.children) {
            Pair<Expression, Expression> recordExprs = createRecordConstructorInner(child);
            StringLiteral fieldName = new StringLiteral(child.name);
            if (recordExprs.getFirst() != null) {
                setRecordArgs.add(new FieldBinding(new LiteralExpr(fieldName), recordExprs.getFirst()));
            }
            if (recordExprs.getSecond() != null) {
                deletionRecordArgs.add(new FieldBinding(new LiteralExpr(fieldName), recordExprs.getSecond()));
            }
        }
        Expression setRecord = setRecordArgs.isEmpty() ? null : new RecordConstructor(setRecordArgs, true);
        Expression deletionRecord =
                deletionRecordArgs.isEmpty() ? null : new RecordConstructor(deletionRecordArgs, true);
        return new Pair<>(setRecord, deletionRecord);
    }

    private class Node {
        private final String name;
        private Expression expr;
        List<Node> children;

        private Node(String name, Expression expr) {
            this.name = name;
            this.expr = expr;
            children = new ArrayList<>();
        }

        boolean hasExpression() {
            return expr != null;
        }

        boolean hasChildren() {
            return children.size() > 0;
        }

        Expression getExpression() {
            return expr;
        }

        void setExpression(Expression expr) {
            this.expr = expr;
        }

        private Node retrieveChild(String childName, boolean createIfEmpty) {
            for (Node child : children) {
                if (child.name.equals(childName)) {
                    return child;
                }
            }

            // If not found and createIfEmpty is true, create and add the child node
            if (createIfEmpty) {
                Node newChild = new Node(childName, null); // New child node with no expression
                children.add(newChild);
                return newChild;
            }
            return null;
        }
    }

}
