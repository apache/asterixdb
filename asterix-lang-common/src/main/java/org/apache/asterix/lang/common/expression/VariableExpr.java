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

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;

public class VariableExpr implements Expression {
    private VarIdentifier var;
    private boolean isNewVar;
    private boolean namedValueAccess = false;

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
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws AsterixException {
        return visitor.visit(this, arg);
    }

    public void setNamedValueAccess(boolean namedValueAccess) {
        this.namedValueAccess = namedValueAccess;
    }

    public boolean namedValueAccess() {
        return namedValueAccess;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof VariableExpr)) {
            return false;
        }
        VariableExpr expr = (VariableExpr) obj;
        return var.equals(expr.getVar());
    }
}
