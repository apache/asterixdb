/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.aql.expression;

import java.util.ArrayList;

import edu.uci.ics.asterix.aql.base.AbstractExpression;
import edu.uci.ics.asterix.aql.base.Expression;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlExpressionVisitor;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlVisitorWithVoidReturn;
import edu.uci.ics.asterix.common.exceptions.AsterixException;

public class OperatorExpr extends AbstractExpression {
    private ArrayList<Expression> exprList;
    private ArrayList<OperatorType> opList;
    private ArrayList<Integer> exprBroadcastIdx;
    private boolean currentop = false;

    public OperatorExpr() {
        super();
        exprList = new ArrayList<Expression>();
        exprBroadcastIdx = new ArrayList<Integer>();
        opList = new ArrayList<OperatorType>();
    }

    public OperatorExpr(ArrayList<Expression> exprList, ArrayList<Integer> exprBroadcastIdx,
            ArrayList<OperatorType> opList) {
        this.exprList = exprList;
        this.exprBroadcastIdx = exprBroadcastIdx;
        this.opList = opList;
    }

    public boolean isCurrentop() {
        return currentop;
    }

    public void setCurrentop(boolean currentop) {
        this.currentop = currentop;
    }

    public ArrayList<Expression> getExprList() {
        return exprList;
    }

    public ArrayList<Integer> getExprBroadcastIdx() {
        return exprBroadcastIdx;
    }

    public ArrayList<OperatorType> getOpList() {
        return opList;
    }

    public void setExprList(ArrayList<Expression> exprList) {
        this.exprList = exprList;
    }

    public void addOperand(Expression operand) {
        addOperand(operand, false);
    }

    public void addOperand(Expression operand, boolean broadcast) {
        if (broadcast) {
            exprBroadcastIdx.add(exprList.size());
        }
        exprList.add(operand);
    }

    public final static boolean opIsComparison(OperatorType t) {
        return t == OperatorType.EQ || t == OperatorType.NEQ || t == OperatorType.GT || t == OperatorType.GE
                || t == OperatorType.LT || t == OperatorType.LE;
    }

    public void addOperator(String strOp) {
        if ("or".equals(strOp))
            opList.add(OperatorType.OR);
        else if ("and".equals(strOp))
            opList.add(OperatorType.AND);
        else if ("<".equals(strOp))
            opList.add(OperatorType.LT);
        else if (">".equals(strOp))
            opList.add(OperatorType.GT);
        else if ("<=".equals(strOp))
            opList.add(OperatorType.LE);
        else if ("<=".equals(strOp))
            opList.add(OperatorType.LE);
        else if (">=".equals(strOp))
            opList.add(OperatorType.GE);
        else if ("=".equals(strOp))
            opList.add(OperatorType.EQ);
        else if ("!=".equals(strOp))
            opList.add(OperatorType.NEQ);
        else if ("+".equals(strOp))
            opList.add(OperatorType.PLUS);
        else if ("-".equals(strOp))
            opList.add(OperatorType.MINUS);
        else if ("*".equals(strOp))
            opList.add(OperatorType.MUL);
        else if ("/".equals(strOp))
            opList.add(OperatorType.DIV);
        else if ("%".equals(strOp))
            opList.add(OperatorType.MOD);
        else if ("^".equals(strOp))
            opList.add(OperatorType.CARET);
        else if ("idiv".equals(strOp))
            opList.add(OperatorType.IDIV);
        else if ("~=".equals(strOp))
            opList.add(OperatorType.FUZZY_EQ);
    }

    @Override
    public Kind getKind() {
        return Kind.OP_EXPRESSION;
    }

    public <T> void accept(IAqlVisitorWithVoidReturn<T> visitor, T arg) throws AsterixException {
        visitor.visit(this, arg);
    }

    @Override
    public <R, T> R accept(IAqlExpressionVisitor<R, T> visitor, T arg) throws AsterixException {
        return visitor.visitOperatorExpr(this, arg);
    }

    public boolean isBroadcastOperand(int idx) {
        for (Integer i : exprBroadcastIdx) {
            if (i == idx) {
                return true;
            }
        }
        return false;
    }
}
