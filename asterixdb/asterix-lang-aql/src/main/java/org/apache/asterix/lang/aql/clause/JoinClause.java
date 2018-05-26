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

import java.util.List;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.lang.aql.visitor.base.IAQLPlusVisitor;
import org.apache.asterix.lang.common.base.AbstractClause;
import org.apache.asterix.lang.common.base.Clause;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;

public class JoinClause extends AbstractClause {

    public static enum JoinKind {
        INNER,
        LEFT_OUTER
    }

    private Expression whereExpr;
    private List<Clause> leftClauses, rightClauses;
    private final JoinKind kind;

    public JoinClause() {
        kind = JoinKind.INNER;
    }

    public JoinClause(JoinKind kind) {
        this.kind = kind;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        return ((IAQLPlusVisitor<R, T>) visitor).visitJoinClause(this, arg);
    }

    @Override
    public ClauseType getClauseType() {
        return null;
    }

    public List<Clause> getLeftClauses() {
        return leftClauses;
    }

    public List<Clause> getRightClauses() {
        return rightClauses;
    }

    public Expression getWhereExpr() {
        return whereExpr;
    }

    public void setLeftClauses(List<Clause> leftClauses) {
        this.leftClauses = leftClauses;
    }

    public void setRightClauses(List<Clause> righClauses) {
        this.rightClauses = righClauses;
    }

    public void setWhereExpr(Expression whereExpr) {
        this.whereExpr = whereExpr;
    }

    public JoinKind getKind() {
        return kind;
    }
}
