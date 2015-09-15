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
package org.apache.asterix.aql.expression;

import org.apache.asterix.aql.base.Expression;
import org.apache.asterix.aql.expression.visitor.IAqlExpressionVisitor;
import org.apache.asterix.aql.expression.visitor.IAqlVisitorWithVoidReturn;
import org.apache.asterix.common.exceptions.AsterixException;

public class VariableExpr implements Expression {
    private VarIdentifier var;
    private boolean isNewVar;

    public VariableExpr() {
        super();
        isNewVar = true;
    }

    public VariableExpr(VarIdentifier var) {
        super();
        this.var = var;
        isNewVar = true;
    }

    public boolean getIsNewVar() {
        return isNewVar;
    }

    public void setIsNewVar(boolean isNewVar) {
        this.isNewVar = isNewVar;
    }

    public VarIdentifier getVar() {
        return var;
    }

    public void setVar(VarIdentifier var) {
        this.var = var;
    }

    @Override
    public Kind getKind() {
        return Kind.VARIABLE_EXPRESSION;
    }

    @Override
    public <T> void accept(IAqlVisitorWithVoidReturn<T> visitor, T arg) throws AsterixException {
        visitor.visit(this, arg);
    }

    @Override
    public <R, T> R accept(IAqlExpressionVisitor<R, T> visitor, T arg) throws AsterixException {
        return visitor.visitVariableExpr(this, arg);
    }
}
