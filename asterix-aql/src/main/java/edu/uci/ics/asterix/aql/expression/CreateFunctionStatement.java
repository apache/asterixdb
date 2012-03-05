package edu.uci.ics.asterix.aql.expression;

import java.util.ArrayList;
import java.util.List;


import edu.uci.ics.asterix.aql.base.Statement;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlExpressionVisitor;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlVisitorWithVoidReturn;
import edu.uci.ics.asterix.aql.util.FunctionUtil;
import edu.uci.ics.asterix.common.exceptions.AsterixException;

public class CreateFunctionStatement implements Statement {

    private FunIdentifier funIdentifier;
    private String functionBody;
    private boolean ifNotExists;
    private List<String> paramList;

    public FunIdentifier getFunctionIdentifier() {
        return funIdentifier;
    }

    public void setFunctionIdentifier(FunIdentifier funIdentifier) {
        this.funIdentifier = funIdentifier;
    }

    public String getFunctionBody() {
        return functionBody;
    }

    public void setFunctionBody(String functionBody) {
        this.functionBody = functionBody;
    }

    public void setIfNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    public CreateFunctionStatement(FunIdentifier funIdentifier, List<VarIdentifier> parameterList, String functionBody,
            boolean ifNotExists) {
        
        this.funIdentifier = funIdentifier;
        this.functionBody = functionBody;
        this.ifNotExists = ifNotExists;
        this.paramList = new ArrayList<String>();
        for (VarIdentifier varId : parameterList) {
            this.paramList.add(varId.getValue());
        }
    }

    public boolean getIfNotExists() {
        return this.ifNotExists;
    }

    @Override
    public Kind getKind() {
        return Kind.CREATE_FUNCTION;
    }

    public List<String> getParamList() {
        return paramList;
    }

    public void setParamList(List<String> paramList) {
        this.paramList = paramList;
    }

    @Override
    public <R, T> R accept(IAqlExpressionVisitor<R, T> visitor, T arg) throws AsterixException {
        return visitor.visit(this, arg);
    }

    @Override
    public <T> void accept(IAqlVisitorWithVoidReturn<T> visitor, T arg) throws AsterixException {
        visitor.visit(this, arg);
    }

}
