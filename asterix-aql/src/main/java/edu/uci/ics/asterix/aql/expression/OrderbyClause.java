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
import edu.uci.ics.asterix.aql.expression.visitor.IAqlVisitorWithVoidReturn;
import edu.uci.ics.asterix.common.exceptions.AsterixException;

public class OrderbyClause implements Clause {
    private List<Expression> orderbyList;
    private List<OrderModifier> modifierList;
    private int numFrames = -1;
    private int numTuples = -1;

    public OrderbyClause() {
    }

    public OrderbyClause(List<Expression> orderbyList, List<OrderModifier> modifierList) {
        this.orderbyList = orderbyList;
        this.modifierList = modifierList;
    }

    public List<Expression> getOrderbyList() {
        return orderbyList;
    }

    public void setOrderbyList(List<Expression> orderbyList) {
        this.orderbyList = orderbyList;
    }

    public List<OrderModifier> getModifierList() {
        return modifierList;
    }

    public void setModifierList(List<OrderModifier> modifierList) {
        this.modifierList = modifierList;
    }

    @Override
    public ClauseType getClauseType() {
        return ClauseType.ORDER_BY_CLAUSE;
    }

    public <T> void accept(IAqlVisitorWithVoidReturn<T> visitor, T arg) throws AsterixException {
        visitor.visit(this, arg);
    }

    public enum OrderModifier {
        ASC,
        DESC
    }

    @Override
    public <R, T> R accept(IAqlExpressionVisitor<R, T> visitor, T arg) throws AsterixException {
        return visitor.visitOrderbyClause(this, arg);
    }

    public int getNumFrames() {
        return numFrames;
    }

    public void setNumFrames(int numFrames) {
        this.numFrames = numFrames;
    }

    public int getNumTuples() {
        return numTuples;
    }

    public void setNumTuples(int numTuples) {
        this.numTuples = numTuples;
    }
}
