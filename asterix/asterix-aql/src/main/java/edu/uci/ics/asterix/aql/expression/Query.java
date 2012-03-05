package edu.uci.ics.asterix.aql.expression;

import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.asterix.aql.base.Expression;
import edu.uci.ics.asterix.aql.base.Statement;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlExpressionVisitor;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlVisitorWithVoidReturn;
import edu.uci.ics.asterix.common.exceptions.AsterixException;

public class Query implements Statement {
    private Expression body;
    private List<Statement> prologDeclList = new ArrayList<Statement>();
    private boolean isDummyQuery = false;

    public Query() {
    }

    public Query(boolean isDummyQuery) {
        this.isDummyQuery = isDummyQuery;
    }

    public boolean isDummyQuery() {
        return isDummyQuery;
    }

    public Expression getBody() {
        return body;
    }

    public void setBody(Expression body) {
        this.body = body;
    }

    public void addPrologDecl(Statement stmt) {
        this.prologDeclList.add(stmt);
    }

    public List<Statement> getPrologDeclList() {
        return prologDeclList;
    }

    public void setPrologDeclList(List<Statement> prologDeclList) {
        this.prologDeclList = prologDeclList;
    }

    // public void addFunctionDecl(FunctionDeclClass fc){
    // if(functionDeclList == null){
    // functionDeclList = new ArrayList<FunctionDeclClass>();
    // }
    // functionDeclList.add(fc);
    // }
    @Override
    public Kind getKind() {
        return Kind.QUERY;
    }

    @Override
    public <T> void accept(IAqlVisitorWithVoidReturn<T> visitor, T step) throws AsterixException {
        visitor.visit(this, step);
    }

    @Override
    public <R, T> R accept(IAqlExpressionVisitor<R, T> visitor, T arg) throws AsterixException {
        return visitor.visitQuery(this, arg);
    }
}
