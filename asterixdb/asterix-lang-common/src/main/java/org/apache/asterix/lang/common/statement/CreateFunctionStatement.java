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
import org.apache.asterix.lang.common.expression.IndexedTypeExpression;
import org.apache.asterix.lang.common.expression.RecordConstructor;
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

    IndexedTypeExpression returnType;
    boolean deterministic;
    boolean nullCall;
    String lang;
    String libName;
    String externalIdent;
    List<Pair<VarIdentifier, IndexedTypeExpression>> args;
    AdmObjectNode resources;

    public String getFunctionBody() {
        return functionBody;
    }

    public CreateFunctionStatement(FunctionSignature signature,
            List<Pair<VarIdentifier, IndexedTypeExpression>> parameterList, IndexedTypeExpression returnType,
            String functionBody, Expression functionBodyExpression, boolean ifNotExists) {
        this.signature = signature;
        this.functionBody = functionBody;
        this.functionBodyExpression = functionBodyExpression;
        this.ifNotExists = ifNotExists;
        this.args = parameterList;
        this.returnType = returnType;
    }

    public CreateFunctionStatement(FunctionSignature signature,
            List<Pair<VarIdentifier, IndexedTypeExpression>> parameterList, IndexedTypeExpression returnType,
            boolean deterministic, boolean nullCall, String lang, String libName, String externalIdent,
            RecordConstructor resources, boolean ifNotExists) throws CompilationException {
        this.signature = signature;
        this.args = parameterList;
        this.returnType = returnType;
        this.deterministic = deterministic;
        this.nullCall = nullCall;
        this.lang = lang;
        this.libName = libName;
        this.externalIdent = externalIdent;
        this.resources = resources == null ? null : ExpressionUtils.toNode(resources);
        functionBody = null;
        this.ifNotExists = ifNotExists;
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

    public Expression getFunctionBodyExpression() {
        return functionBodyExpression;
    }

    public IndexedTypeExpression getReturnType() {
        return returnType;
    }

    public boolean isDeterministic() {
        return deterministic;
    }

    public boolean isNullCall() {
        return nullCall;
    }

    public boolean isExternal() {
        return externalIdent != null;
    }

    public boolean isUnknownable() {
        return returnType == null || returnType.isUnknownable();
    }

    public String getLang() {
        return lang;
    }

    public String getLibName() {
        return libName;
    }

    public String getExternalIdent() {
        return externalIdent;
    }

    public List<Pair<VarIdentifier, IndexedTypeExpression>> getArgs() {
        return args;
    }

    public Map<String, String> getResources() throws CompilationException {
        return resources != null ? ConfigurationUtil.toProperties(resources) : Collections.EMPTY_MAP;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        return visitor.visit(this, arg);
    }

    @Override
    public byte getCategory() {
        return Category.DDL;
    }

}
