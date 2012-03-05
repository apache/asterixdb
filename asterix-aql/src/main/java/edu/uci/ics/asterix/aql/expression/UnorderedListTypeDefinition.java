package edu.uci.ics.asterix.aql.expression;

import edu.uci.ics.asterix.aql.expression.visitor.IAqlExpressionVisitor;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlVisitorWithVoidReturn;
import edu.uci.ics.asterix.common.exceptions.AsterixException;

public class UnorderedListTypeDefinition extends TypeExpression {

    private TypeExpression itemTypeExpression;

    public UnorderedListTypeDefinition(TypeExpression itemTypeExpression) {
        this.itemTypeExpression = itemTypeExpression;
    }

    @Override
    public TypeExprKind getTypeKind() {
        return TypeExprKind.UNORDEREDLIST;
    }

    @Override
    public <R, T> R accept(IAqlExpressionVisitor<R, T> visitor, T arg) throws AsterixException {
        return visitor.visitUnorderedListTypeDefiniton(this, arg);
    }

    @Override
    public <T> void accept(IAqlVisitorWithVoidReturn<T> visitor, T arg) throws AsterixException {
        visitor.visit(this, arg);
    }

    public TypeExpression getItemTypeExpression() {
        return itemTypeExpression;
    }

}
