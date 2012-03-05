package edu.uci.ics.asterix.aql.expression;

import edu.uci.ics.asterix.aql.base.Statement;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlExpressionVisitor;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlVisitorWithVoidReturn;
import edu.uci.ics.asterix.common.exceptions.AsterixException;

public class DataverseDropStatement implements Statement {

    private Identifier dataverseName;
    private boolean ifExists;

    public DataverseDropStatement(Identifier dataverseName, boolean ifExists) {
        this.dataverseName = dataverseName;
        this.ifExists = ifExists;
    }

    @Override
    public Kind getKind() {
        return Kind.DATAVERSE_DROP;
    }

    public Identifier getDataverseName() {
        return dataverseName;
    }

    public boolean getIfExists() {
        return ifExists;
    }

    @Override
    public <R, T> R accept(IAqlExpressionVisitor<R, T> visitor, T arg) throws AsterixException {
        return visitor.visitDataverseDropStatement(this, arg);
    }

    @Override
    public <T> void accept(IAqlVisitorWithVoidReturn<T> visitor, T arg) throws AsterixException {
        visitor.visit(this, arg);
    }

}
