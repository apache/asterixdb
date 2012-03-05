package edu.uci.ics.asterix.aql.expression;

import java.util.List;

import edu.uci.ics.asterix.aql.base.Expression;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlExpressionVisitor;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlVisitorWithVoidReturn;
import edu.uci.ics.asterix.common.exceptions.AsterixException;

public class RecordConstructor implements Expression {
    private List<FieldBinding> fbList;

    public RecordConstructor() {
        super();
    }

    public RecordConstructor(List<FieldBinding> fbList) {
        super();
        this.fbList = fbList;
    }

    public List<FieldBinding> getFbList() {
        return fbList;
    }

    public void setFbList(List<FieldBinding> fbList) {
        this.fbList = fbList;
    }

    @Override
    public Kind getKind() {
        return Kind.RECORD_CONSTRUCTOR_EXPRESSION;
    }

    @Override
    public <T> void accept(IAqlVisitorWithVoidReturn<T> visitor, T arg) throws AsterixException {
        visitor.visit(this, arg);
    }

    @Override
    public <R, T> R accept(IAqlExpressionVisitor<R, T> visitor, T arg) throws AsterixException {
        return visitor.visitRecordConstructor(this, arg);
    }
}
