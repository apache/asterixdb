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
import edu.uci.ics.asterix.aql.base.Clause.ClauseType;
import edu.uci.ics.asterix.aql.base.Expression;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlExpressionVisitor;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlVisitorWithVoidReturn;
import edu.uci.ics.asterix.common.exceptions.AsterixException;

public class FLWOGRExpression implements Expression {
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
    public <T> void accept(IAqlVisitorWithVoidReturn<T> visitor, T arg) throws AsterixException {
        visitor.visit(this, arg);

    }

    @Override
    public <R, T> R accept(IAqlExpressionVisitor<R, T> visitor, T arg) throws AsterixException {
        return visitor.visitFlworExpression(this, arg);
    }
}
