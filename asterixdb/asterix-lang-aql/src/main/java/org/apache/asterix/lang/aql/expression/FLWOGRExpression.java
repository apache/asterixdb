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
package org.apache.asterix.lang.aql.expression;

import java.util.List;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.lang.aql.visitor.base.IAQLVisitor;
import org.apache.asterix.lang.common.base.AbstractExpression;
import org.apache.asterix.lang.common.base.Clause;
import org.apache.asterix.lang.common.base.Clause.ClauseType;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;

public class FLWOGRExpression extends AbstractExpression {
    private List<Clause> clauseList;
    private Expression returnExpr;

    public FLWOGRExpression() {
        super();
    }

    public FLWOGRExpression(List<Clause> clauseList, Expression returnExpr) {
        super();
        this.clauseList = clauseList;
        this.returnExpr = returnExpr;
    }

    public List<Clause> getClauseList() {
        return clauseList;
    }

    public void setClauseList(List<Clause> clauseList) {
        this.clauseList = clauseList;
    }

    public Expression getReturnExpr() {
        return returnExpr;
    }

    public void setReturnExpr(Expression returnExpr) {
        this.returnExpr = returnExpr;
    }

    @Override
    public Kind getKind() {
        return Kind.FLWOGR_EXPRESSION;
    }

    public boolean noForClause() {
        for (Clause c : clauseList) {
            if (c.getClauseType() == ClauseType.FOR_CLAUSE) {
                return false;
            }
        }
        return true;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        return ((IAQLVisitor<R, T>) visitor).visit(this, arg);
    }
}
