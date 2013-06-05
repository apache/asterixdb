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

import edu.uci.ics.asterix.aql.base.Clause;
import edu.uci.ics.asterix.aql.base.Expression;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlExpressionVisitor;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlVisitorWithVoidReturn;
import edu.uci.ics.asterix.common.exceptions.AsterixException;

public class LimitClause implements Clause {
    private Expression limitexpr;
    private Expression offset;

    public LimitClause() {
    }

    public LimitClause(Expression limitexpr, Expression offset) {
        this.limitexpr = limitexpr;
        this.offset = offset;
    }

    public Expression getLimitExpr() {
        return limitexpr;
    }

    public void setLimitExpr(Expression limitexpr) {
        this.limitexpr = limitexpr;
    }

    public Expression getOffset() {
        return offset;
    }

    public void setOffset(Expression offset) {
        this.offset = offset;
    }

    @Override
    public ClauseType getClauseType() {
        return ClauseType.LIMIT_CLAUSE;
    }

    @Override
    public <T> void accept(IAqlVisitorWithVoidReturn<T> visitor, T arg) throws AsterixException {
        visitor.visit(this, arg);
    }

    @Override
    public <R, T> R accept(IAqlExpressionVisitor<R, T> visitor, T arg) throws AsterixException {
        return visitor.visitLimitClause(this, arg);
    }
}
