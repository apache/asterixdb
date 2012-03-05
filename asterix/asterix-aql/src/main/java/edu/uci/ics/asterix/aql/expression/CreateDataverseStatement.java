package edu.uci.ics.asterix.aql.expression;

import edu.uci.ics.asterix.aql.base.Statement;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlExpressionVisitor;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlVisitorWithVoidReturn;
import edu.uci.ics.asterix.common.exceptions.AsterixException;

public class CreateDataverseStatement implements Statement {

    private Identifier dataverseName;
    private String format;
    private boolean ifNotExists;

    public CreateDataverseStatement(Identifier dataverseName, String format, boolean ifNotExists) {
        this.dataverseName = dataverseName;
        if (format == null)
            this.format = "edu.uci.ics.asterix.runtime.formats.NonTaggedDataFormat";
        else
            this.format = format;
        this.ifNotExists = ifNotExists;
    }

    public Identifier getDataverseName() {
        return dataverseName;
    }

    public String getFormat() {
        return format;
    }

    public boolean getIfNotExists() {
        return this.ifNotExists;
    }

    @Override
    public Kind getKind() {
        return Kind.CREATE_DATAVERSE;
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
