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

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.aql.base.Statement;
import org.apache.asterix.aql.expression.visitor.IAqlExpressionVisitor;
import org.apache.asterix.aql.expression.visitor.IAqlVisitorWithVoidReturn;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.functions.FunctionSignature;

public class CreateFunctionStatement implements Statement {

    private final FunctionSignature signature;
    private final String functionBody;
    private final boolean ifNotExists;
    private final boolean isProcedure;
    private final List<String> paramList;

    public FunctionSignature getaAterixFunction() {
        return signature;
    }

    public String getFunctionBody() {
        return functionBody;
    }

    public CreateFunctionStatement(FunctionSignature signature, List<VarIdentifier> parameterList, String functionBody,
            boolean ifNotExists, boolean isProcedure) {
        this.signature = signature;
        this.functionBody = functionBody;
        this.ifNotExists = ifNotExists;
        this.isProcedure = isProcedure;
        this.paramList = new ArrayList<String>();
        for (VarIdentifier varId : parameterList) {
            this.paramList.add(varId.getValue());
        }
    }

    public boolean getIfNotExists() {
        return this.ifNotExists;
    }

    public boolean getIsProcedure() {
        return this.isProcedure;
    }

    @Override
    public Kind getKind() {
        return Kind.CREATE_FUNCTION;
    }

    public List<String> getParamList() {
        return paramList;
    }

    public FunctionSignature getSignature() {
        return signature;
    }

    @Override
    public <R, T> R accept(IAqlExpressionVisitor<R, T> visitor, T arg) throws AsterixException {
        return visitor.visit(this, arg);
    }

    @Override
    public <T> void accept(IAqlVisitorWithVoidReturn<T> visitor, T arg) throws AsterixException {
        visitor.visit(this, arg);
    }

}
