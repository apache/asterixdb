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

import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.lang.common.base.AbstractExpression;
import org.apache.asterix.lang.common.base.Expression;

public abstract class AbstractCallExpression extends AbstractExpression {

    protected FunctionSignature functionSignature;

    protected List<Expression> exprList;

    protected Expression aggFilterExpr;

    protected AbstractCallExpression(FunctionSignature functionSignature, List<Expression> exprList,
            Expression aggFilterExpr) {
        this.functionSignature = Objects.requireNonNull(functionSignature);
        this.exprList = Objects.requireNonNull(exprList);
        this.aggFilterExpr = aggFilterExpr;
    }

    public FunctionSignature getFunctionSignature() {
        return functionSignature;
    }

    public void setFunctionSignature(FunctionSignature functionSignature) {
        this.functionSignature = Objects.requireNonNull(functionSignature);
    }

    public List<Expression> getExprList() {
        return exprList;
    }

    public void setExprList(List<Expression> exprList) {
        this.exprList = Objects.requireNonNull(exprList);
    }

    public boolean hasAggregateFilterExpr() {
        return aggFilterExpr != null;
    }

    public Expression getAggregateFilterExpr() {
        return aggFilterExpr;
    }

    public void setAggregateFilterExpr(Expression aggFilterExpr) {
        this.aggFilterExpr = aggFilterExpr;
    }
}
