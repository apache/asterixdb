package edu.uci.ics.asterix.aql.expression;

import edu.uci.ics.asterix.aql.base.Expression;
import edu.uci.ics.asterix.aql.base.ILiteral;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlExpressionVisitor;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlVisitorWithVoidReturn;
import edu.uci.ics.asterix.common.exceptions.AsterixException;

public class LiteralExpr implements Expression {
    private ILiteral value;

    public LiteralExpr() {
    }

    public LiteralExpr(ILiteral value) {
        this.value = value;
    }

    public ILiteral getValue() {
        return value;
    }

    public void setValue(ILiteral value) {
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
