package edu.uci.ics.asterix.aql.expression;

import edu.uci.ics.asterix.aql.base.Statement;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlExpressionVisitor;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlVisitorWithVoidReturn;
import edu.uci.ics.asterix.common.exceptions.AsterixException;

public class WriteStatement implements Statement {

    private final Identifier ncName;
    private final String fileName;
    private final String writerClassName;

    public WriteStatement(Identifier ncName, String fileName, String writerClassName) {
        this.ncName = ncName;
        this.fileName = fileName;
        this.writerClassName = writerClassName;
    }

    public Identifier getNcName() {
        return ncName;
    }

    public String getFileName() {
        return fileName;
    }

    public String getWriterClassName() {
        return writerClassName;
    }

    @Override
    public Kind getKind() {
        return Kind.WRITE;
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
