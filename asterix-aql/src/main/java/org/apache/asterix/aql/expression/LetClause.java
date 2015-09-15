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

public class LetClause implements Clause {
    private VariableExpr varExpr;
    private Expression bindExpr;

    public LetClause() {
        super();
    }

    public LetClause(VariableExpr varExpr, Expression bindExpr) {
        super();
        this.varExpr = varExpr;
        this.bindExpr = bindExpr;
    }

    public VariableExpr getVarExpr() {
        return varExpr;
    }

    public void setVarExpr(VariableExpr varExpr) {
        this.varExpr = varExpr;
    }

    public Expression getBindingExpr() {
        return bindExpr;
    }

    public void setBindingExpr(Expression bindExpr) {
        this.bindExpr = bindExpr;
    }

    public void setBeExpr(Expression beExpr) {
        this.bindExpr = beExpr;
    }

    @Override
    public ClauseType getClauseType() {
        return ClauseType.LET_CLAUSE;
    }

    public <T> void accept(IAqlVisitorWithVoidReturn<T> visitor, T arg) throws AsterixException {
        visitor.visit(this, arg);
    }

    @Override
    public <R, T> R accept(IAqlExpressionVisitor<R, T> visitor, T arg) throws AsterixException {
        return visitor.visitLetClause(this, arg);
    }

}
