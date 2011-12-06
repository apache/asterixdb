package edu.uci.ics.hyracks.algebricks.examples.piglet.ast;

public abstract class ASTNode {
    public enum Tag {
        ASSIGNMENT,
        DUMP,
        LOAD,
        FILTER,

        SCALAR_FUNCTION,
        LITERAL,
        FIELD_ACCESS,
    }

    public abstract Tag getTag();
}