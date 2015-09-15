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

public class ForClause implements Clause {
    private VariableExpr varExpr;
    private VariableExpr posExpr;
    private Expression inExpr;

    public ForClause() {
        super();
    }

    public ForClause(VariableExpr varExpr, Expression inExpr) {
        super();
        this.varExpr = varExpr;
        this.inExpr = inExpr;
    }

    public VariableExpr getVarExpr() {
        return varExpr;
    }

    public void setVarExpr(VariableExpr varExpr) {
        this.varExpr = varExpr;
    }

    public Expression getInExpr() {
        return inExpr;
    }

    public void setInExpr(Expression inExpr) {
        this.inExpr = inExpr;
    }

    @Override
    public ClauseType getClauseType() {
        return ClauseType.FOR_CLAUSE;
    }

    @Override
    public <T> void accept(IAqlVisitorWithVoidReturn<T> visitor, T arg) throws AsterixException {
        visitor.visit(this, arg);
    }

    @Override
    public <R, T> R accept(IAqlExpressionVisitor<R, T> visitor, T arg) throws AsterixException {
        return visitor.visitForClause(this, arg);
    }

    public void setPosExpr(VariableExpr posExpr) {
        this.posExpr = posExpr;
    }

    public VariableExpr getPosVarExpr() {
        return posExpr;
    }
}
