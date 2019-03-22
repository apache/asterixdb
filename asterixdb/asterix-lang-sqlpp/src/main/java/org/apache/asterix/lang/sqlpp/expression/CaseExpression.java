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

package org.apache.asterix.lang.sqlpp.expression;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.lang.common.base.AbstractExpression;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.literal.NullLiteral;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.asterix.lang.sqlpp.visitor.base.ISqlppVisitor;

public class CaseExpression extends AbstractExpression {

    private Expression conditionExpr;
    private List<Expression> whenExprs = new ArrayList<>();
    private List<Expression> thenExprs = new ArrayList<>();
    private Expression elseExpr; // elseExpr could be null.

    public CaseExpression(Expression conditionExpr, List<Expression> whenExprs, List<Expression> thenExprs,
            Expression elseExpr) {
        this.conditionExpr = conditionExpr;
        this.whenExprs.addAll(whenExprs);
        this.thenExprs.addAll(thenExprs);

        // If no when expression matches the case expr, we return null
        this.elseExpr = elseExpr == null ? new LiteralExpr(NullLiteral.INSTANCE) : elseExpr;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        return ((ISqlppVisitor<R, T>) visitor).visit(this, arg);
    }

    @Override
    public Kind getKind() {
        return Kind.CASE_EXPRESSION;
    }

    public Expression getConditionExpr() {
        return conditionExpr;
    }

    public Expression getElseExpr() {
        return elseExpr;
    }

    public List<Expression> getWhenExprs() {
        return whenExprs;
    }

    public List<Expression> getThenExprs() {
        return thenExprs;
    }

    public void setConditionExpr(Expression conditionExpr) {
        this.conditionExpr = conditionExpr;
    }

    public void setElseExpr(Expression elseExpr) {
        this.elseExpr = elseExpr;
    }

    public void setWhenExprs(List<Expression> whenExprs) {
        this.whenExprs = whenExprs;
    }

    public void setThenExprs(List<Expression> thenExprs) {
        this.thenExprs = thenExprs;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("CASE ");
        sb.append(conditionExpr);
        sb.append("\n");
        for (int index = 0; index < whenExprs.size(); ++index) {
            sb.append("WHEN ");
            sb.append(whenExprs.get(index));
            sb.append(" THEN ");
            sb.append(thenExprs.get(index));
            sb.append("\n");
        }
        sb.append("ELSE ");
        sb.append(elseExpr);
        sb.append("END\n");
        return sb.toString();
    }

    @Override
    public int hashCode() {
        return Objects.hash(conditionExpr, elseExpr, thenExprs, whenExprs);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof CaseExpression)) {
            return false;
        }
        CaseExpression target = (CaseExpression) object;
        return Objects.equals(conditionExpr, target.conditionExpr) && Objects.equals(elseExpr, target.elseExpr)
                && Objects.equals(thenExprs, target.thenExprs) && Objects.equals(thenExprs, thenExprs);
    }
}
