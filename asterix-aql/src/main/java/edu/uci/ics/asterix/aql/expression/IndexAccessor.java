package edu.uci.ics.asterix.aql.expression;

import edu.uci.ics.asterix.aql.base.Expression;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlExpressionVisitor;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlVisitorWithVoidReturn;
import edu.uci.ics.asterix.common.exceptions.AsterixException;

public class IndexAccessor extends AbstractAccessor {
    private boolean any;
    private int index;

    public final static int ANY = -1;

    public IndexAccessor(Expression expr, int index) {
        super(expr);
        if (index == -1)
            this.any = true;
        this.index = index;
    }

    public boolean isAny() {
        return any;
    }

    public void setAny(boolean any) {
        this.any = any;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    @Override
    public Kind getKind() {
        return Kind.INDEX_ACCESSOR_EXPRESSION;
    }

    @Override
    public <T> void accept(IAqlVisitorWithVoidReturn<T> visitor, T arg) throws AsterixException {
        visitor.visit(this, arg);
    }

    @Override
    public <R, T> R accept(IAqlExpressionVisitor<R, T> visitor, T arg) throws AsterixException {
        return visitor.visitIndexAccessor(this, arg);
    }
}
