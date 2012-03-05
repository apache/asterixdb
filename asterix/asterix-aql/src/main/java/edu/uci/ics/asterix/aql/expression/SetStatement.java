package edu.uci.ics.asterix.aql.expression;

import edu.uci.ics.asterix.aql.base.Statement;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlExpressionVisitor;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlVisitorWithVoidReturn;
import edu.uci.ics.asterix.common.exceptions.AsterixException;

public class SetStatement implements Statement {

    private String propName;
    private String propValue;

    public SetStatement(String propName, String propValue) {
        this.propName = propName;
        this.propValue = propValue;
    }

    public String getPropName() {
        return propName;
    }

    public String getPropValue() {
        return propValue;
    }

    @Override
    public Kind getKind() {
        return Kind.SET;
    }

    @Override
    public <R, T> R accept(IAqlExpressionVisitor<R, T> visitor, T arg) throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public <T> void accept(IAqlVisitorWithVoidReturn<T> visitor, T arg) throws AsterixException {
        visitor.visit(this, arg);
    }

}
