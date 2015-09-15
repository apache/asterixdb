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
package org.apache.asterix.aql.expression;

import org.apache.asterix.aql.base.Clause;
import org.apache.asterix.aql.base.Expression;
import org.apache.asterix.aql.expression.visitor.IAqlExpressionVisitor;
import org.apache.asterix.aql.expression.visitor.IAqlVisitorWithVoidReturn;
import org.apache.asterix.common.exceptions.AsterixException;

public class UpdateClause implements Clause {

    private Expression target;
    private Expression value;
    private InsertStatement is;
    private DeleteStatement ds;
    private UpdateStatement us;
    private Expression condition;
    private UpdateClause ifbranch;
    private UpdateClause elsebranch;

    public UpdateClause(Expression target, Expression value, InsertStatement is, DeleteStatement ds,
            UpdateStatement us, Expression condition, UpdateClause ifbranch, UpdateClause elsebranch) {
        this.target = target;
        this.value = value;
        this.is = is;
        this.ds = ds;
        this.us = us;
        this.condition = condition;
        this.ifbranch = ifbranch;
        this.elsebranch = elsebranch;
    }

    public Expression getTarget() {
        return target;
    }

    public Expression getValue() {
        return value;
    }

    public InsertStatement getInsertStatement() {
        return is;
    }

    public DeleteStatement getDeleteStatement() {
        return ds;
    }

    public UpdateStatement getUpdateStatement() {
        return us;
    }

    public Expression getCondition() {
        return condition;
    }

    public UpdateClause getIfBranch() {
        return ifbranch;
    }

    public UpdateClause getElseBranch() {
        return elsebranch;
    }

    @Override
    public ClauseType getClauseType() {
        return ClauseType.UPDATE_CLAUSE;
    }

    @Override
    public <R, T> R accept(IAqlExpressionVisitor<R, T> visitor, T arg) throws AsterixException {
        return visitor.visitUpdateClause(this, arg);
    }

    @Override
    public <T> void accept(IAqlVisitorWithVoidReturn<T> visitor, T arg) throws AsterixException {
        visitor.visit(this, arg);
    }

}
