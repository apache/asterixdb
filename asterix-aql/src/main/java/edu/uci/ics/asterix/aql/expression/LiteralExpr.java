package edu.uci.ics.asterix.aql.expression;

import edu.uci.ics.asterix.aql.base.Expression;
import edu.uci.ics.asterix.aql.base.Literal;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlExpressionVisitor;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlVisitorWithVoidReturn;
import edu.uci.ics.asterix.common.exceptions.AsterixException;

public class LiteralExpr implements Expression {
    private Literal value;

    public LiteralExpr() {
    }

    public LiteralExpr(Literal value) {
        this.value = value;
    }

    public Literal getValue() {
        return value;
    }

    public void setValue(Literal value) {
        this.value = value;
    }

    @Override
    public Kind getKind() {
        return Kind.LITERAL_EXPRESSION;
    }

    public <T> void accept(IAqlVisitorWithVoidReturn<T> visitor, T arg) throws AsterixException {
        visitor.visit(this, arg);
    }

    @Override
    public <R, T> R accept(IAqlExpressionVisitor<R, T> visitor, T arg) throws AsterixException {
        return visitor.visitLiteralExpr(this, arg);
    }

}
