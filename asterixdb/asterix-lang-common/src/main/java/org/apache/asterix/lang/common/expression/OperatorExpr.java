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
import java.util.Optional;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.lang.common.base.AbstractExpression;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.struct.OperatorType;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.commons.lang3.ObjectUtils;

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

    public void addOperator(String strOp) throws CompilationException {
        OperatorType op = OperatorType.fromSymbol(strOp);
        if (op == null) {
            throw new CompilationException("Unsupported operator: " + strOp);
        }
        addOperator(op);
    }

    public void addOperator(OperatorType op) throws CompilationException {
        if (op == null) {
            throw new NullPointerException();
        }
        opList.add(op);
    }

    @Override
    public Kind getKind() {
        return Kind.OP_EXPRESSION;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
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

    @Override
    public int hashCode() {
        return ObjectUtils.hashCodeMulti(currentop, exprBroadcastIdx, exprList, opList);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof OperatorExpr)) {
            return false;
        }
        OperatorExpr target = (OperatorExpr) object;
        return currentop == target.isCurrentop() && ObjectUtils.equals(exprBroadcastIdx, target.exprBroadcastIdx)
                && ObjectUtils.equals(exprList, target.exprList) && ObjectUtils.equals(opList, target.opList);
    }
}
