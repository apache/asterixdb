package edu.uci.ics.asterix.aql.expression;

import edu.uci.ics.asterix.aql.base.IAqlExpression;

public abstract class TypeExpression implements IAqlExpression {

    public enum TypeExprKind {
        RECORD,
        TYPEREFERENCE,
        ORDEREDLIST,
        UNORDEREDLIST
    }

    public abstract TypeExprKind getTypeKind();

}
