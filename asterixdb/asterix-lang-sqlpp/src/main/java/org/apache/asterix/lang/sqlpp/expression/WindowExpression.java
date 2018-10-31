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

package org.apache.asterix.lang.sqlpp.expression;

import java.util.List;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.lang.common.base.AbstractExpression;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.clause.OrderbyClause;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.asterix.lang.sqlpp.visitor.base.ISqlppVisitor;

public class WindowExpression extends AbstractExpression {
    private Expression expr;
    private List<Expression> partitionList;
    private List<Expression> orderbyList;
    private List<OrderbyClause.OrderModifier> orderbyModifierList;

    public WindowExpression(Expression expr, List<Expression> partitionList, List<Expression> orderbyList,
            List<OrderbyClause.OrderModifier> orderbyModifierList) {
        if (expr == null || orderbyList == null) {
            throw new NullPointerException();
        }
        this.expr = expr;
        this.partitionList = partitionList;
        this.orderbyList = orderbyList;
        this.orderbyModifierList = orderbyModifierList;
    }

    @Override
    public Kind getKind() {
        return Kind.WINDOW_EXPRESSION;
    }

    public Expression getExpr() {
        return expr;
    }

    public void setExpr(Expression expr) {
        if (expr == null) {
            throw new NullPointerException();
        }
        this.expr = expr;
    }

    public boolean hasPartitionList() {
        return partitionList != null && !partitionList.isEmpty();
    }

    public List<Expression> getPartitionList() {
        return partitionList;
    }

    public void setPartitionList(List<Expression> partitionList) {
        if (partitionList == null) {
            throw new NullPointerException();
        }
        this.partitionList = partitionList;
    }

    public List<Expression> getOrderbyList() {
        return orderbyList;
    }

    public void setOrderbyList(List<Expression> orderbyList) {
        if (orderbyList == null) {
            throw new NullPointerException();
        }
        this.orderbyList = orderbyList;
    }

    public List<OrderbyClause.OrderModifier> getOrderbyModifierList() {
        return orderbyModifierList;
    }

    public void setOrderbyModifierList(List<OrderbyClause.OrderModifier> orderbyModifierList) {
        if (orderbyModifierList == null) {
            throw new NullPointerException();
        }
        this.orderbyModifierList = orderbyModifierList;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        return ((ISqlppVisitor<R, T>) visitor).visit(this, arg);
    }
}
