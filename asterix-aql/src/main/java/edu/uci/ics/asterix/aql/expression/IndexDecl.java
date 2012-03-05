package edu.uci.ics.asterix.aql.expression;

public class IndexDecl extends CreateIndexStatement {
    @Override
    public Kind getKind() {
        return Kind.INDEX_DECL;
    }
}
