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
package org.apache.asterix.lang.common.clause;

import java.util.Objects;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.lang.common.base.AbstractClause;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.statement.DeleteStatement;
import org.apache.asterix.lang.common.statement.InsertStatement;
import org.apache.asterix.lang.common.statement.UpdateStatement;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;

public class UpdateClause extends AbstractClause {

    private Expression target;
    private Expression value;
    private InsertStatement is;
    private DeleteStatement ds;
    private UpdateStatement us;
    private Expression condition;
    private UpdateClause ifbranch;
    private UpdateClause elsebranch;

    public UpdateClause(Expression target, Expression value, InsertStatement is, DeleteStatement ds, UpdateStatement us,
            Expression condition, UpdateClause ifbranch, UpdateClause elsebranch) {
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
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        return visitor.visit(this, arg);
    }

    public boolean hasSet() {
        return target != null;
    }

    public boolean hasInsert() {
        return is != null;
    }

    public boolean hasDelete() {
        return ds != null;
    }

    public boolean hasUpdate() {
        return us != null;
    }

    public boolean hasIfElse() {
        return ifbranch != null;
    }

    public boolean hasElse() {
        return elsebranch != null;
    }

    @Override
    public int hashCode() {
        return Objects.hash(condition, ds, elsebranch, ifbranch, is, target, us, value);
    }

    @Override
    @SuppressWarnings("squid:S1067") // expressions should not be too complex
    public boolean equals(Object object) {
        if (this == object) {
            return false;
        }
        if (!(object instanceof UpdateClause)) {
            return false;
        }
        UpdateClause other = (UpdateClause) object;
        return Objects.equals(condition, other.condition) && Objects.equals(ds, other.ds)
                && Objects.equals(elsebranch, other.elsebranch) && Objects.equals(ifbranch, other.ifbranch)
                && Objects.equals(is, other.is) && Objects.equals(target, other.target) && Objects.equals(us, other.us)
                && Objects.equals(value, other.value);
    }

}
