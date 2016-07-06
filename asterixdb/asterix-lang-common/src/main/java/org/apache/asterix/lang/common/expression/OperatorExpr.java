/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.lang.common.expression;

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.lang.common.base.AbstractExpression;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.struct.OperatorType;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;

public class OperatorExpr extends AbstractExpression {
    private List<Expression> exprList;
    private List<OperatorType> opList;
    private List<Integer> exprBroadcastIdx;
    private boolean currentop = false;

    public OperatorExpr() {
        super();
        exprList = new ArrayList<>();
        exprBroadcastIdx = new ArrayList<>();
        opList = new ArrayList<>();
    }

    public OperatorExpr(List<Expression> exprList, List<Integer> exprBroadcastIdx, List<OperatorType> opList,
            boolean currentop) {
        this.exprList = exprList;
        this.exprBroadcastIdx = exprBroadcastIdx;
        this.opList = opList;
        this.currentop = currentop;
    }

    public boolean isCurrentop() {
        return currentop;
    }

    public void setCurrentop(boolean currentop) {
        this.currentop = currentop;
    }

    public List<Expression> getExprList() {
        return exprList;
    }

    public List<Integer> getExprBroadcastIdx() {
        return exprBroadcastIdx;
    }

    public List<OperatorType> getOpList() {
        return opList;
    }

    public void setExprList(List<Expression> exprList) {
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

    public static final boolean opIsComparison(OperatorType t) {
        boolean cmp = t == OperatorType.EQ || t == OperatorType.NEQ || t == OperatorType.GT;
        cmp = cmp || t == OperatorType.GE || t == OperatorType.LT || t == OperatorType.LE;
        return cmp;
    }

    public void addOperator(String strOp) {
        switch (strOp) {
            case "or":
                opList.add(OperatorType.OR);
                break;
            case "and":
                opList.add(OperatorType.AND);
                break;
            case "<":
                opList.add(OperatorType.LT);
                break;
            case ">":
                opList.add(OperatorType.GT);
                break;
            case "<=":
                opList.add(OperatorType.LE);
                break;
            case ">=":
                opList.add(OperatorType.GE);
                break;
            case "=":
                opList.add(OperatorType.EQ);
                break;
            case "!=":
                opList.add(OperatorType.NEQ);
                break;
            case "+":
                opList.add(OperatorType.PLUS);
                break;
            case "-":
                opList.add(OperatorType.MINUS);
                break;
            case "*":
                opList.add(OperatorType.MUL);
                break;
            case "/":
                opList.add(OperatorType.DIV);
                break;
            case "%":
                opList.add(OperatorType.MOD);
                break;
            case "^":
                opList.add(OperatorType.CARET);
                break;
            case "idiv":
                opList.add(OperatorType.IDIV);
                break;
            case "~=":
                opList.add(OperatorType.FUZZY_EQ);
                break;
            case "like":
                opList.add(OperatorType.LIKE);
                break;
            case "not_like":
                opList.add(OperatorType.NOT_LIKE);
                break;
            case "in":
                opList.add(OperatorType.IN);
                break;
            case "not_in":
                opList.add(OperatorType.NOT_IN);
                break;
            default:
                throw new UnsupportedOperationException("Unsupported operator: " + strOp);
        }

    }

    @Override
    public Kind getKind() {
        return Kind.OP_EXPRESSION;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws AsterixException {
        return visitor.visit(this, arg);
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
