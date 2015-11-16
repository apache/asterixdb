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

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.clause.LimitClause;
import org.apache.asterix.lang.common.clause.OrderbyClause;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.asterix.lang.sqlpp.clause.SelectSetOperation;
import org.apache.asterix.lang.sqlpp.visitor.base.ISqlppVisitor;

public class SelectExpression implements Expression {

    private List<LetClause> letList;
    private SelectSetOperation selectSetOperation;
    private OrderbyClause orderbyClause;
    private LimitClause limitClause;
    private boolean subquery;

    public SelectExpression(List<LetClause> letList, SelectSetOperation selectSetOperation, OrderbyClause orderbyClause,
            LimitClause limitClause, boolean subquery) {
        this.letList = letList;
        this.selectSetOperation = selectSetOperation;
        this.orderbyClause = orderbyClause;
        this.limitClause = limitClause;
        this.subquery = subquery;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws AsterixException {
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
        return letList != null && letList.size() > 0;
    }

    public boolean isSubquery() {
        return subquery;
    }

}
