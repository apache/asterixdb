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
package org.apache.asterix.lang.common.statement;

import java.util.List;
import java.util.Objects;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.lang.common.base.AbstractStatement;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.lang.common.clause.UpdateClause;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;

public class UpdateStatement extends AbstractStatement {

    private VariableExpr vars;
    private Expression target;
    private Expression condition;
    private List<UpdateClause> ucs;

    public UpdateStatement(VariableExpr vars, Expression target, Expression condition, List<UpdateClause> ucs) {
        this.vars = vars;
        this.target = target;
        this.condition = condition;
        this.ucs = ucs;
    }

    @Override
    public Kind getKind() {
        return Statement.Kind.UPDATE;
    }

    public VariableExpr getVariableExpr() {
        return vars;
    }

    public Expression getTarget() {
        return target;
    }

    public Expression getCondition() {
        return condition;
    }

    public List<UpdateClause> getUpdateClauses() {
        return ucs;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        return visitor.visit(this, arg);
    }

    @Override
    public int hashCode() {
        return Objects.hash(condition, target, ucs, vars);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof UpdateStatement)) {
            return false;
        }
        UpdateStatement update = (UpdateStatement) object;
        return Objects.equals(condition, update.condition) && Objects.equals(target, update.target)
                && Objects.equals(ucs, update.ucs) && Objects.equals(vars, update.vars);
    }

    @Override
    public byte getCategory() {
        return Category.UPDATE;
    }

}
