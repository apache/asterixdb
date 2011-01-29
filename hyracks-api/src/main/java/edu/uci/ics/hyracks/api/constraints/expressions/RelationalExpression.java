/*
 * Copyright 2009-2010 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.api.constraints.expressions;

import java.util.Collection;

public class RelationalExpression extends ConstraintExpression {
    private static final long serialVersionUID = 1L;

    public enum Operator {
        EQUAL,
        NOT_EQUAL,
        LESS,
        LESS_EQUAL,
        GREATER,
        GREATER_EQUAL
    }

    private final ConstraintExpression left;
    private final ConstraintExpression right;
    private final Operator op;

    public RelationalExpression(ConstraintExpression left, ConstraintExpression right, Operator op) {
        this.left = left;
        this.right = right;
        this.op = op;
    }

    @Override
    public ExpressionTag getTag() {
        return ExpressionTag.RELATIONAL;
    }

    public ConstraintExpression getLeft() {
        return left;
    }

    public ConstraintExpression getRight() {
        return right;
    }

    public Operator getOperator() {
        return op;
    }

    @Override
    public void getChildren(Collection<ConstraintExpression> children) {
        children.add(left);
        children.add(right);
    }

    @Override
    protected void toString(StringBuilder buffer) {
        buffer.append(getTag()).append('(').append(op).append(", ");
        left.toString(buffer);
        buffer.append(", ");
        right.toString(buffer);
        buffer.append(')');
    }
}