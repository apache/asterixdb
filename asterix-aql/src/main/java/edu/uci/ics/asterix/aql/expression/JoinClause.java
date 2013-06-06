/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.aql.expression;

import java.util.List;

import edu.uci.ics.asterix.aql.base.Clause;
import edu.uci.ics.asterix.aql.base.Expression;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlExpressionVisitor;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlPlusExpressionVisitor;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlVisitorWithVoidReturn;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.hyracks.algebricks.common.exceptions.NotImplementedException;

public class JoinClause implements Clause {

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
    public <R, T> R accept(IAqlExpressionVisitor<R, T> visitor, T arg) throws AsterixException {
        return ((IAqlPlusExpressionVisitor<R, T>) visitor).visitJoinClause(this, arg);
    }

    @Override
    public <T> void accept(IAqlVisitorWithVoidReturn<T> visitor, T arg) throws AsterixException {
        throw new NotImplementedException();
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
