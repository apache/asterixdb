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
import java.util.Objects;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.lang.common.base.AbstractExpression;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.struct.OperatorType;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;

public class OperatorExpr extends AbstractExpression {
    private List<Expression> exprList;
    private List<OperatorType> opList;
    private boolean currentop;

    public OperatorExpr() {
        super();
        exprList = new ArrayList<>();
        opList = new ArrayList<>();
    }

    public OperatorExpr(List<Expression> exprList, List<OperatorType> opList, boolean currentop) {
        this.exprList = exprList;
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

    public List<OperatorType> getOpList() {
        return opList;
    }

    public void setExprList(List<Expression> exprList) {
        this.exprList = exprList;
    }

    public void addOperand(Expression operand) {
        exprList.add(operand);
    }

    public static boolean opIsComparison(OperatorType t) {
        switch (t) {
            case EQ:
            case NEQ:
            case GT:
            case GE:
            case LT:
            case LE:
                return true;
            default:
                return false;
        }
    }

    public void addOperator(String strOp) throws CompilationException {
        OperatorType op = OperatorType.fromSymbol(strOp);
        if (op == null) {
            throw new CompilationException("Unsupported operator: " + strOp);
        }
        addOperator(op);
    }

    public void addOperator(OperatorType op) {
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

    @Override
    public int hashCode() {
        return Objects.hash(currentop, exprList, opList);
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
        return currentop == target.isCurrentop() && Objects.equals(exprList, target.exprList)
                && Objects.equals(opList, target.opList);
    }
}
