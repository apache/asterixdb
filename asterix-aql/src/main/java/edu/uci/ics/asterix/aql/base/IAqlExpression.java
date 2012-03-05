package edu.uci.ics.asterix.aql.base;

import edu.uci.ics.asterix.aql.expression.visitor.IAqlExpressionVisitor;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlVisitorWithVoidReturn;
import edu.uci.ics.asterix.common.exceptions.AsterixException;

public interface IAqlExpression {
    public <R, T> R accept(IAqlExpressionVisitor<R, T> visitor, T arg) throws AsterixException;

    public <T> void accept(IAqlVisitorWithVoidReturn<T> visitor, T arg) throws AsterixException;
}
