package edu.uci.ics.asterix.aql.expression;

import edu.uci.ics.asterix.aql.base.Statement;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlExpressionVisitor;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlVisitorWithVoidReturn;
import edu.uci.ics.asterix.common.annotations.TypeDataGen;
import edu.uci.ics.asterix.common.exceptions.AsterixException;

public class TypeDecl implements Statement {

    private final Identifier ident;
    private final TypeExpression typeDef;
    private final TypeDataGen datagenAnnotation;
    private final boolean ifNotExists;

    public TypeDecl(Identifier ident, TypeExpression typeDef, TypeDataGen datagen, boolean ifNotExists) {
        this.ident = ident;
        this.typeDef = typeDef;
        this.datagenAnnotation = datagen;
        this.ifNotExists = ifNotExists;
    }

    public TypeDecl(Identifier ident, TypeExpression typeDef) {
        this(ident, typeDef, null, false);
    }

    public Identifier getIdent() {
        return ident;
    }

    public TypeExpression getTypeDef() {
        return typeDef;
    }

    public boolean getIfNotExists() {
        return this.ifNotExists;
    }

    @Override
    public Kind getKind() {
        return Kind.TYPE_DECL;
    }

    @Override
    public <R, T> R accept(IAqlExpressionVisitor<R, T> visitor, T arg) throws AsterixException {
        return visitor.visitTypeDecl(this, arg);
    }

    @Override
    public <T> void accept(IAqlVisitorWithVoidReturn<T> visitor, T arg) throws AsterixException {
        visitor.visit(this, arg);
    }

    public TypeDataGen getDatagenAnnotation() {
        return datagenAnnotation;
    }

}
