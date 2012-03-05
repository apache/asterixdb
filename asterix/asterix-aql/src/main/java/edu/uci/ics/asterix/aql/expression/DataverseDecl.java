package edu.uci.ics.asterix.aql.expression;

import edu.uci.ics.asterix.aql.base.Statement;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlExpressionVisitor;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlVisitorWithVoidReturn;
import edu.uci.ics.asterix.common.exceptions.AsterixException;

public class DataverseDecl implements Statement {

    private Identifier dataverseName;

    public DataverseDecl(Identifier dataverseName) {
        this.dataverseName = dataverseName;
    }

    public Identifier getDataverseName() {
        return dataverseName;
    }

    @Override
    public Kind getKind() {
        return Kind.DATAVERSE_DECL;
    }

    @Override
    public <R, T> R accept(IAqlExpressionVisitor<R, T> visitor, T arg) throws AsterixException {
        // TODO
        return null;
    }

    @Override
    public <T> void accept(IAqlVisitorWithVoidReturn<T> visitor, T arg) throws AsterixException {
        visitor.visit(this, arg);
    }

}
