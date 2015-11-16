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

package org.apache.asterix.lang.sqlpp.clause;

import java.util.List;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.lang.common.base.Clause;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.asterix.lang.sqlpp.visitor.base.ISqlppVisitor;

public class FromTerm implements Clause {
    private Expression leftExpr;
    private VariableExpr leftVar;
    private VariableExpr posVar;
    private List<AbstractBinaryCorrelateClause> correlateClauses;

    public FromTerm(Expression leftExpr, VariableExpr leftVar, VariableExpr posVar,
            List<AbstractBinaryCorrelateClause> correlateClauses) {
        this.leftExpr = leftExpr;
        this.leftVar = leftVar;
        this.posVar = posVar;
        this.correlateClauses = correlateClauses;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws AsterixException {
        return ((ISqlppVisitor<R, T>) visitor).visit(this, arg);
    }

    @Override
    public ClauseType getClauseType() {
        return ClauseType.FROM_TERM;
    }

    public Expression getLeftExpression() {
        return leftExpr;
    }

    public void setLeftExpression(Expression expr) {
        this.leftExpr = expr;
    }

    public VariableExpr getLeftVariable() {
        return leftVar;
    }

    public VariableExpr getPositionalVariable() {
        return posVar;
    }

    public boolean hasCorrelateClauses() {
        return correlateClauses != null && correlateClauses.size() > 0;
    }

    public List<AbstractBinaryCorrelateClause> getCorrelateClauses() {
        return correlateClauses;
    }

    public boolean hasPositionalVariable() {
        return posVar != null;
    }

}
