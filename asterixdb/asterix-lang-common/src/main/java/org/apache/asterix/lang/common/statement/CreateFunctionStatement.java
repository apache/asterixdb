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

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.lang.common.base.AbstractStatement;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.lang.common.expression.RecordConstructor;
import org.apache.asterix.lang.common.expression.TypeExpression;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.common.util.ConfigurationUtil;
import org.apache.asterix.lang.common.util.ExpressionUtils;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.asterix.object.base.AdmObjectNode;
import org.apache.hyracks.algebricks.common.utils.Pair;

public class CreateFunctionStatement extends AbstractStatement {

    private final FunctionSignature signature;
    private final String functionBody;
    private final Expression functionBodyExpression;
    private final boolean ifNotExists;
    private final List<Pair<VarIdentifier, TypeExpression>> paramList;
    private final TypeExpression returnType;

    private final String lang;
    private final String libName;
    private final List<String> externalIdentifier;
    private final Boolean deterministic;
    private final Boolean nullCall;
    private final AdmObjectNode resources;

    public CreateFunctionStatement(FunctionSignature signature, List<Pair<VarIdentifier, TypeExpression>> paramList,
            String functionBody, Expression functionBodyExpression, boolean ifNotExists) {
        this.signature = signature;
        this.functionBody = functionBody;
        this.functionBodyExpression = functionBodyExpression;
        this.ifNotExists = ifNotExists;
        this.paramList = requireNullTypes(paramList);
        this.returnType = null; // return type specification is not allowed for inline functions
        this.lang = null;
        this.libName = null;
        this.externalIdentifier = null;
        this.deterministic = null;
        this.nullCall = null;
        this.resources = null;
    }

    public CreateFunctionStatement(FunctionSignature signature, List<Pair<VarIdentifier, TypeExpression>> paramList,
            TypeExpression returnType, boolean deterministic, boolean nullCall, String lang, String libName,
            List<String> externalIdentifier, RecordConstructor resources, boolean ifNotExists)
            throws CompilationException {
        this.signature = signature;
        this.ifNotExists = ifNotExists;
        this.paramList = paramList;
        this.returnType = returnType;
        this.deterministic = deterministic;
        this.nullCall = nullCall;
        this.lang = lang;
        this.libName = libName;
        this.externalIdentifier = externalIdentifier;
        this.resources = resources == null ? null : ExpressionUtils.toNode(resources);
        this.functionBody = null;
        this.functionBodyExpression = null;
    }

    public boolean getIfNotExists() {
        return this.ifNotExists;
    }

    @Override
    public Kind getKind() {
        return Statement.Kind.CREATE_FUNCTION;
    }

    public FunctionSignature getFunctionSignature() {
        return signature;
    }

    public String getFunctionBody() {
        return functionBody;
    }

    public Expression getFunctionBodyExpression() {
        return functionBodyExpression;
    }

    public List<Pair<VarIdentifier, TypeExpression>> getParameters() {
        return paramList;
    }

    public TypeExpression getReturnType() {
        return returnType;
    }

    public boolean isExternal() {
        return externalIdentifier != null;
    }

    public List<String> getExternalIdentifier() {
        return externalIdentifier;
    }

    public String getLibName() {
        return libName;
    }

    public String getLang() {
        return lang;
    }

    public Boolean getDeterministic() {
        return deterministic;
    }

    public Boolean getNullCall() {
        return nullCall;
    }

    public Map<String, String> getResources() throws CompilationException {
        return resources != null ? ConfigurationUtil.toProperties(resources) : Collections.emptyMap();
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        return visitor.visit(this, arg);
    }

    @Override
    public byte getCategory() {
        return Category.DDL;
    }

    private static List<Pair<VarIdentifier, TypeExpression>> requireNullTypes(
            List<Pair<VarIdentifier, TypeExpression>> paramList) {
        for (Pair<VarIdentifier, TypeExpression> p : paramList) {
            if (p.second != null) {
                throw new IllegalArgumentException(String.valueOf(p.second));
            }
        }
        return paramList;
    }
}
