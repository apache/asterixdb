package edu.uci.ics.asterix.aql.expression;

import java.util.List;

import edu.uci.ics.asterix.aql.base.AbstractExpression;
import edu.uci.ics.asterix.aql.base.Expression;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlExpressionVisitor;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlVisitorWithVoidReturn;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.common.functions.FunctionSignature;

public class CallExpr extends AbstractExpression {
    private final FunctionSignature functionSignature;
    private List<Expression> exprList;
    private boolean isBuiltin;

    public CallExpr(FunctionSignature functionSignature, List<Expression> exprList) {
        this.functionSignature = functionSignature;
        this.exprList = exprList;
    }

    public FunctionSignature getFunctionSignature() {
        return functionSignature;
    }

    public List<Expression> getExprList() {
        return exprList;
    }

    public boolean isBuiltin() {
        return isBuiltin;
    }

    @Override
    public Kind getKind() {
        return Kind.CALL_EXPRESSION;
    }

    public void setExprList(List<Expression> exprList) {
        this.exprList = exprList;
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
