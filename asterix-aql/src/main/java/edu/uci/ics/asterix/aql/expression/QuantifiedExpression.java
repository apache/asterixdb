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

import edu.uci.ics.asterix.aql.base.Expression;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlExpressionVisitor;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlVisitorWithVoidReturn;
import edu.uci.ics.asterix.common.exceptions.AsterixException;

public class QuantifiedExpression implements Expression {
    private List<QuantifiedPair> quantifiedList;
    private Expression satisfiesExpr;
    private Quantifier quantifier;

    public QuantifiedExpression() {
        super();
    }

    public QuantifiedExpression(Quantifier quantifier, List<QuantifiedPair> quantifiedList, Expression satisfiesExpr) {
        super();
        this.quantifier = quantifier;
        this.quantifiedList = quantifiedList;
        this.satisfiesExpr = satisfiesExpr;
    }

    public Quantifier getQuantifier() {
        return quantifier;
    }

    public void setQuantifier(Quantifier quantifier) {
        this.quantifier = quantifier;
    }

    public List<QuantifiedPair> getQuantifiedList() {
        return quantifiedList;
    }

    public void setQuantifiedList(List<QuantifiedPair> quantifiedList) {
        this.quantifiedList = quantifiedList;
    }

    public Expression getSatisfiesExpr() {
        return satisfiesExpr;
    }

    public void setSatisfiesExpr(Expression satisfiesExpr) {
        this.satisfiesExpr = satisfiesExpr;
    }

    @Override
    public Kind getKind() {
        return Kind.QUANTIFIED_EXPRESSION;
    }

    public <T> void accept(IAqlVisitorWithVoidReturn<T> visitor, T arg) throws AsterixException {
        visitor.visit(this, arg);

    }

    @Override
    public <R, T> R accept(IAqlExpressionVisitor<R, T> visitor, T arg) throws AsterixException {
        return visitor.visitQuantifiedExpression(this, arg);
    }

    public enum Quantifier {
        EVERY,
        SOME
    }
}
