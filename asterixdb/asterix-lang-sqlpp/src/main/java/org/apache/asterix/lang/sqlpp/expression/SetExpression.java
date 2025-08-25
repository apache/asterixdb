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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.lang.common.base.AbstractExpression;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.asterix.lang.sqlpp.visitor.base.ISqlppVisitor;

/**
 * Represents a SET clause in an UPDATE statement.
 * <p>A SetExpression contains pairs of path expressions and value expressions,
 * where each pair represents a field assignment: path = value.
 * <p>
 * <b>Example:</b>
 * UPDATE users AS u
 * SET u.name = "John", u.age = 30, u.status = "active"
 * WHERE u.id = 1
 * This creates a SetExpression with:
 * <ul>
 *   <li>pathExpr: [u.name, u.age, u.status]</li>
 *   <li>valueExpr: ["John", 30, "active"]</li>
 * </ul>
 */
public class SetExpression extends AbstractExpression {
    private List<Expression> pathExpr;
    private List<Expression> valueExpr;

    public SetExpression(List<Expression> pathExpr, List<Expression> valueExpr) {
        this.pathExpr = pathExpr != null ? pathExpr : new ArrayList<>();
        this.valueExpr = valueExpr != null ? valueExpr : new ArrayList<>();
    }

    public List<Expression> getPathExprList() {
        return pathExpr;
    }

    public List<Expression> getValueExprList() {
        return valueExpr;
    }

    public void setPathExprList(List<Expression> pathExpr) {
        this.pathExpr = pathExpr;
    }

    public void setValueExprList(List<Expression> valueExpr) {
        this.valueExpr = valueExpr;
    }

    public void add(Expression path, Expression value) {
        pathExpr.add(path);
        valueExpr.add(value);
    }

    public int size() {
        return pathExpr.size();
    }

    @Override
    public Kind getKind() {
        return Kind.UPDATE_SET_EXPRESSION;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        return ((ISqlppVisitor<R, T>) visitor).visit(this, arg);
    }

    @Override
    public int hashCode() {
        return Objects.hash(pathExpr, valueExpr);
    }

    @Override
    @SuppressWarnings("squid:S1067")
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof SetExpression)) {
            return false;
        }
        SetExpression other = (SetExpression) object;
        return Objects.equals(other.pathExpr, this.pathExpr) && Objects.equals(other.valueExpr, this.valueExpr);
    }
}