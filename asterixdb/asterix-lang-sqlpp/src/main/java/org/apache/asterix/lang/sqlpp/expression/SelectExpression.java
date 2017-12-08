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
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.clause.LimitClause;
import org.apache.asterix.lang.common.clause.OrderbyClause;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.asterix.lang.sqlpp.clause.SelectSetOperation;
import org.apache.asterix.lang.sqlpp.visitor.base.ISqlppVisitor;

public class SelectExpression extends AbstractExpression {

    private List<LetClause> letList = new ArrayList<>();
    private SelectSetOperation selectSetOperation;
    private OrderbyClause orderbyClause;
    private LimitClause limitClause;
    private boolean subquery;

    public SelectExpression(List<LetClause> letList, SelectSetOperation selectSetOperation, OrderbyClause orderbyClause,
            LimitClause limitClause, boolean subquery) {
        if (letList != null) {
            this.letList.addAll(letList);
        }
        this.selectSetOperation = selectSetOperation;
        this.orderbyClause = orderbyClause;
        this.limitClause = limitClause;
        this.subquery = subquery;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        return ((ISqlppVisitor<R, T>) visitor).visit(this, arg);
    }

    @Override
    public Kind getKind() {
        return Kind.SELECT_EXPRESSION;
    }

    public List<LetClause> getLetList() {
        return letList;
    }

    public SelectSetOperation getSelectSetOperation() {
        return selectSetOperation;
    }

    public OrderbyClause getOrderbyClause() {
        return orderbyClause;
    }

    public LimitClause getLimitClause() {
        return limitClause;
    }

    public boolean hasOrderby() {
        return orderbyClause != null;
    }

    public boolean hasLimit() {
        return limitClause != null;
    }

    public boolean hasLetClauses() {
        return letList != null && !letList.isEmpty();
    }

    public boolean isSubquery() {
        return subquery;
    }

    public void setSubquery(boolean setSubquery) {
        subquery = setSubquery;
    }

    @Override
    public int hashCode() {
        return Objects.hash(letList, limitClause, orderbyClause, selectSetOperation, subquery);
    }

    @Override
    @SuppressWarnings("squid:S1067") // expressions should not be too complex
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof SelectExpression)) {
            return false;
        }
        SelectExpression target = (SelectExpression) object;
        return Objects.equals(letList, target.letList) && Objects.equals(limitClause, target.limitClause)
                && Objects.equals(orderbyClause, target.orderbyClause) && subquery == target.subquery
                && Objects.equals(selectSetOperation, target.selectSetOperation);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(subquery ? "(" : "");
        if (this.hasLetClauses()) {
            sb.append(letList.toString());
        }
        sb.append(selectSetOperation);
        if (hasOrderby()) {
            sb.append(orderbyClause);
        }
        if (hasLimit()) {
            sb.append(limitClause);
        }
        sb.append(subquery ? ")" : "");
        return sb.toString();
    }
}
