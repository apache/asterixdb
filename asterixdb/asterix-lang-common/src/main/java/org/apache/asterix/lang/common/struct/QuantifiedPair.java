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
package org.apache.asterix.lang.common.struct;

import java.util.Objects;

import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.expression.VariableExpr;

public class QuantifiedPair {
    private VariableExpr varExpr;
    private Expression expr;

    public QuantifiedPair() {
        // default constructor
    }

    public QuantifiedPair(VariableExpr varExpr, Expression expr) {
        this.varExpr = varExpr;
        this.expr = expr;
    }

    public VariableExpr getVarExpr() {
        return varExpr;
    }

    public void setVarExpr(VariableExpr varExpr) {
        this.varExpr = varExpr;
    }

    public Expression getExpr() {
        return expr;
    }

    public void setExpr(Expression expr) {
        this.expr = expr;
    }

    @Override
    public int hashCode() {
        return Objects.hash(expr, varExpr);
    }

    @Override
    public boolean equals(Object object) {
        if (!(object instanceof QuantifiedPair)) {
            return false;
        }
        QuantifiedPair target = (QuantifiedPair) object;
        return Objects.equals(expr, target.expr) && Objects.equals(varExpr, target.varExpr);
    }
}
