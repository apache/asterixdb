package edu.uci.ics.asterix.aql.expression;

import java.util.List;

import edu.uci.ics.asterix.aql.base.Expression;
import edu.uci.ics.asterix.aql.base.Statement;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlExpressionVisitor;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlVisitorWithVoidReturn;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.common.functions.FunctionSignature;

public class FunctionDecl implements Statement {
    private FunctionSignature signature;
    private List<VarIdentifier> paramList;
    private Expression funcBody;

    public FunctionDecl(FunctionSignature signature, List<VarIdentifier> paramList, Expression funcBody) {
        this.signature = signature;
        this.paramList = paramList;
        this.funcBody = funcBody;
    }

    public FunctionSignature getSignature() {
        return signature;
    }

    public List<VarIdentifier> getParamList() {
        return paramList;
    }

    public Expression getFuncBody() {
        return funcBody;
    }

    public void setFuncBody(Expression funcBody) {
        this.funcBody = funcBody;
    }

    public void setSignature(FunctionSignature signature) {
        this.signature = signature;
    }

    public void setParamList(List<VarIdentifier> paramList) {
        this.paramList = paramList;
    }

    @Override
    public int hashCode() {
        return signature.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        return (o instanceof FunctionDecl && ((FunctionDecl) o).getSignature().equals(signature));
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
