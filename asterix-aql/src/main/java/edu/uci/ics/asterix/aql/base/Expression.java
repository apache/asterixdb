package edu.uci.ics.asterix.aql.base;

public interface Expression extends IAqlExpression {
    public abstract Kind getKind();

    public enum Kind {
        LITERAL_EXPRESSION,
        FLWOGR_EXPRESSION,
        IF_EXPRESSION,
        QUANTIFIED_EXPRESSION,
        // PARENTHESIZED_EXPRESSION,
        LIST_CONSTRUCTOR_EXPRESSION,
        RECORD_CONSTRUCTOR_EXPRESSION,
        VARIABLE_EXPRESSION,
        METAVARIABLE_EXPRESSION,
        CALL_EXPRESSION,
        OP_EXPRESSION,
        FIELD_ACCESSOR_EXPRESSION,
        INDEX_ACCESSOR_EXPRESSION,
        UNARY_EXPRESSION,
        UNION_EXPRESSION
    }

}
