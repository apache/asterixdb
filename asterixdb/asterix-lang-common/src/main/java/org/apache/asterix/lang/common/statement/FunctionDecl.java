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
package org.apache.asterix.lang.common.statement;

import java.util.List;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.lang.common.base.AbstractStatement;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;

public class FunctionDecl extends AbstractStatement {
    private FunctionSignature signature;
    private List<VarIdentifier> paramList;
    private Expression funcBody;

    public FunctionDecl(FunctionSignature signature, List<VarIdentifier> paramList, Expression funcBody) {
        this.signature = signature;
        this.paramList = paramList;
        this.funcBody = funcBody;
    }

    public FunctionSignature getSignature() {
        return signature;
    }

    public List<VarIdentifier> getParamList() {
        return paramList;
    }

    public Expression getFuncBody() {
        return funcBody;
    }

    public void setFuncBody(Expression funcBody) {
        this.funcBody = funcBody;
    }

    public void setSignature(FunctionSignature signature) {
        this.signature = signature;
    }

    public void setParamList(List<VarIdentifier> paramList) {
        this.paramList = paramList;
    }

    @Override
    public int hashCode() {
        return signature.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        return (o instanceof FunctionDecl && ((FunctionDecl) o).getSignature().equals(signature));
    }

    @Override
    public Kind getKind() {
        return Statement.Kind.FUNCTION_DECL;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        return visitor.visit(this, arg);
    }

    @Override
    public byte getCategory() {
        return Category.QUERY;
    }
}
