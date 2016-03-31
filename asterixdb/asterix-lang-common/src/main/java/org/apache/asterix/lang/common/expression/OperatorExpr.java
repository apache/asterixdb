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
        exprList = new ArrayList<Expression>();
        exprBroadcastIdx = new ArrayList<Integer>();
        opList = new ArrayList<OperatorType>();
    }

    public OperatorExpr(List<Expression> exprList, List<Integer> exprBroadcastIdx, List<OperatorType> opList) {
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
