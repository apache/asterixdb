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

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.lang.common.base.AbstractExpression;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;

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
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws AsterixException {
        return visitor.visit(this, arg);
    }
}
