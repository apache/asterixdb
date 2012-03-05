package edu.uci.ics.asterix.aql.expression;

import java.util.List;

import edu.uci.ics.asterix.aql.base.Expression;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlExpressionVisitor;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlVisitorWithVoidReturn;
import edu.uci.ics.asterix.common.exceptions.AsterixException;

public class ListConstructor implements Expression {
    private List<Expression> exprList;
    private Type type;

    public ListConstructor() {
    }

    public ListConstructor(Type type, List<Expression> exprList) {
        this.type = type;
        this.exprList = exprList;
    }

    public List<Expression> getExprList() {
        return exprList;
    }

    public void setExprList(List<Expression> exprList) {
        this.exprList = exprList;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    @Override
    public Kind getKind() {
        return Kind.LIST_CONSTRUCTOR_EXPRESSION;
    }

    public enum Type {
        ORDERED_LIST_CONSTRUCTOR,
        UNORDERED_LIST_CONSTRUCTOR
    }

    @Override
    public <T> void accept(IAqlVisitorWithVoidReturn<T> visitor, T arg) throws AsterixException {
        visitor.visit(this, arg);

    }

    @Override
    public <R, T> R accept(IAqlExpressionVisitor<R, T> visitor, T arg) throws AsterixException {
        return visitor.visitListConstructor(this, arg);
    }
}
