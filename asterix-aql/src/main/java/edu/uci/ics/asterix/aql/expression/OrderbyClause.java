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
