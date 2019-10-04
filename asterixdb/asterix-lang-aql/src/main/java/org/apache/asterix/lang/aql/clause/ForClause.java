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
package org.apache.asterix.lang.aql.clause;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.lang.aql.visitor.base.IAQLVisitor;
import org.apache.asterix.lang.common.base.AbstractClause;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;

public class ForClause extends AbstractClause {
    private VariableExpr varExpr = null;
    private VariableExpr posExpr = null;
    private Expression inExpr = null;

    public ForClause() {
    }

    public ForClause(VariableExpr varExpr, Expression inExpr) {
        this.varExpr = varExpr;
        this.inExpr = inExpr;
    }

    public ForClause(VariableExpr varExpr, Expression inExpr, VariableExpr posExpr) {
        this.varExpr = varExpr;
        this.inExpr = inExpr;
        this.posExpr = posExpr;
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
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        return ((IAQLVisitor<R, T>) visitor).visit(this, arg);
    }

    public void setPosExpr(VariableExpr posExpr) {
        this.posExpr = posExpr;
    }

    public VariableExpr getPosVarExpr() {
        return posExpr;
    }

    public boolean hasPosVar() {
        return posExpr != null;
    }
}
