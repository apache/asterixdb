package edu.uci.ics.asterix.aql.expression;

public enum OperatorType {
    OR,
    AND,
    LT,
    GT,
    LE,
    GE,
    EQ,
    NEQ,
    PLUS,
    MINUS,
    MUL,
    DIV, // float/double
         // divide
    MOD,
    CARET,
    IDIV, // integer divide
    FUZZY_EQ
}
