package edu.uci.ics.asterix.aql.expression;

import java.util.List;

import edu.uci.ics.asterix.aql.base.Expression;
import edu.uci.ics.asterix.aql.base.Statement;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlExpressionVisitor;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlVisitorWithVoidReturn;
import edu.uci.ics.asterix.common.exceptions.AsterixException;

public class FunctionDecl implements Statement {
    private FunIdentifier ident;
    private List<VarIdentifier> paramList;
    private Expression funcBody;

    public FunctionDecl() {
    }

    public FunctionDecl(FunIdentifier ident, List<VarIdentifier> paramList, Expression funcBody) {
        this.ident = ident;
        this.paramList = paramList;
        this.funcBody = funcBody;
    }

    public FunIdentifier getIdent() {
        return ident;
    }

    public void setIdent(FunIdentifier ident) {
        this.ident = ident;
    }

    public List<VarIdentifier> getParamList() {
        return paramList;
    }

    public void setParamList(List<VarIdentifier> paramList) {
        this.paramList = paramList;
    }

    public Expression getFuncBody() {
        return funcBody;
    }

    public void setFuncBody(Expression funcBody) {
        this.funcBody = funcBody;
    }

    @Override
    public Kind getKind() {
        return Kind.FUNCTION_DECL;
    }

    @Override
    public <T> void accept(IAqlVisitorWithVoidReturn<T> visitor, T arg) throws AsterixException {
        visitor.visit(this, arg);
    }

    @Override
    public <R, T> R accept(IAqlExpressionVisitor<R, T> visitor, T arg) throws AsterixException {
        return visitor.visitFunctionDecl(this, arg);
    }
}
