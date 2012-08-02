package edu.uci.ics.asterix.aql.expression;

import java.util.List;

import edu.uci.ics.asterix.aql.base.AbstractExpression;
import edu.uci.ics.asterix.aql.base.Expression;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlExpressionVisitor;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlVisitorWithVoidReturn;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.om.functions.AsterixFunction;

public class CallExpr extends AbstractExpression {
    private AsterixFunction ident;
    private List<Expression> exprList;
    private boolean isBuiltin;    
    
    public CallExpr() {
    }

    public CallExpr(AsterixFunction ident, List<Expression> exprList) {
        this.ident = ident;
        this.exprList = exprList;
    }

    public AsterixFunction getIdent() {
        return ident;
    }

    public void setIdent(AsterixFunction ident) {
        this.ident = ident;
    }

    public List<Expression> getExprList() {
        return exprList;
    }
          
    public void setExprList(List<Expression> exprList) {
        this.exprList = exprList;
    }

    public boolean isBuiltin() {
        return isBuiltin;
    }

    public void setIsBuiltin(boolean builtin) {
        this.isBuiltin = builtin;
    }

    @Override
    public Kind getKind() {
        return Kind.CALL_EXPRESSION;
    }

    @Override
    public <T> void accept(IAqlVisitorWithVoidReturn<T> visitor, T arg) throws AsterixException {
        visitor.visit(this, arg);
    }

    @Override
    public <R, T> R accept(IAqlExpressionVisitor<R, T> visitor, T arg) throws AsterixException {
        return visitor.visitCallExpr(this, arg);
    }
}
