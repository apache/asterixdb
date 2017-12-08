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

import java.util.List;
import java.util.Objects;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.lang.common.base.AbstractExpression;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.struct.QuantifiedPair;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;

public class QuantifiedExpression extends AbstractExpression {
    private List<QuantifiedPair> quantifiedList;
    private Expression satisfiesExpr;
    private Quantifier quantifier;

    public QuantifiedExpression() {
        super();
    }

    public QuantifiedExpression(Quantifier quantifier, List<QuantifiedPair> quantifiedList, Expression satisfiesExpr) {
        super();
        this.quantifier = quantifier;
        this.quantifiedList = quantifiedList;
        this.satisfiesExpr = satisfiesExpr;
    }

    public Quantifier getQuantifier() {
        return quantifier;
    }

    public void setQuantifier(Quantifier quantifier) {
        this.quantifier = quantifier;
    }

    public List<QuantifiedPair> getQuantifiedList() {
        return quantifiedList;
    }

    public void setQuantifiedList(List<QuantifiedPair> quantifiedList) {
        this.quantifiedList = quantifiedList;
    }

    public Expression getSatisfiesExpr() {
        return satisfiesExpr;
    }

    public void setSatisfiesExpr(Expression satisfiesExpr) {
        this.satisfiesExpr = satisfiesExpr;
    }

    @Override
    public Kind getKind() {
        return Kind.QUANTIFIED_EXPRESSION;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        return visitor.visit(this, arg);
    }

    public enum Quantifier {
        EVERY,
        SOME
    }

    @Override
    public int hashCode() {
        return Objects.hash(quantifiedList, quantifier, satisfiesExpr);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof QuantifiedExpression)) {
            return false;
        }
        QuantifiedExpression target = (QuantifiedExpression) object;
        return Objects.equals(quantifiedList, target.quantifiedList) && Objects.equals(quantifier, target.quantifier)
                && Objects.equals(satisfiesExpr, target.satisfiesExpr);
    }
}
