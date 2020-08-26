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
import java.util.Map;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.lang.common.base.AbstractStatement;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.lang.common.expression.RecordConstructor;
import org.apache.asterix.lang.common.expression.TypeExpression;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.common.util.ConfigurationUtil;
import org.apache.asterix.lang.common.util.ExpressionUtils;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.asterix.object.base.AdmBooleanNode;
import org.apache.asterix.object.base.AdmObjectNode;
import org.apache.asterix.object.base.AdmStringNode;
import org.apache.asterix.object.base.IAdmNode;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.algebricks.common.utils.Pair;

public class CreateFunctionStatement extends AbstractStatement {

    private static final String NULLCALL_FIELD_NAME = "null-call";
    private static final boolean NULLCALL_DEFAULT = false;
    private static final String DETERMINISTIC_FIELD_NAME = "deterministic";
    private static final boolean DETERMINISTIC_DEFAULT = true;
    private static final String RESOURCES_FIELD_NAME = "resources";

    private final FunctionSignature signature;
    private final String functionBody;
    private final Expression functionBodyExpression;
    private final List<Pair<VarIdentifier, TypeExpression>> paramList;
    private final TypeExpression returnType;

    private final DataverseName libraryDataverseName;
    private final String libraryName;
    private final List<String> externalIdentifier;
    private final AdmObjectNode options;

    private final boolean replaceIfExists;
    private final boolean ifNotExists;

    public CreateFunctionStatement(FunctionSignature signature, List<Pair<VarIdentifier, TypeExpression>> paramList,
            String functionBody, Expression functionBodyExpression, boolean replaceIfExists, boolean ifNotExists) {
        this.signature = signature;
        this.functionBody = functionBody;
        this.functionBodyExpression = functionBodyExpression;
        this.paramList = requireNullTypes(paramList); // parameter type specification is not allowed for inline functions
        this.returnType = null; // return type specification is not allowed for inline functions
        this.libraryDataverseName = null;
        this.libraryName = null;
        this.externalIdentifier = null;
        this.options = null;
        this.replaceIfExists = replaceIfExists;
        this.ifNotExists = ifNotExists;
    }

    public CreateFunctionStatement(FunctionSignature signature, List<Pair<VarIdentifier, TypeExpression>> paramList,
            TypeExpression returnType, DataverseName libraryDataverseName, String libraryName,
            List<String> externalIdentifier, RecordConstructor options, boolean replaceIfExists, boolean ifNotExists)
            throws CompilationException {
        this.signature = signature;
        this.paramList = paramList;
        this.returnType = returnType;
        this.libraryDataverseName = libraryDataverseName;
        this.libraryName = libraryName;
        this.externalIdentifier = externalIdentifier;
        this.options = options == null ? null : ExpressionUtils.toNode(options);
        this.functionBody = null;
        this.functionBodyExpression = null;
        this.replaceIfExists = replaceIfExists;
        this.ifNotExists = ifNotExists;
    }

    public boolean getReplaceIfExists() {
        return replaceIfExists;
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

    public DataverseName getLibraryDataverseName() {
        return libraryDataverseName;
    }

    public String getLibraryName() {
        return libraryName;
    }

    public boolean getNullCall() throws CompilationException {
        Boolean nullCall = getBooleanOption(NULLCALL_FIELD_NAME);
        return nullCall != null ? nullCall : NULLCALL_DEFAULT;
    }

    public boolean getDeterministic() throws CompilationException {
        Boolean deterministic = getBooleanOption(DETERMINISTIC_FIELD_NAME);
        return deterministic != null ? deterministic : DETERMINISTIC_DEFAULT;
    }

    private Boolean getBooleanOption(String optionName) throws CompilationException {
        IAdmNode value = getOption(optionName);
        if (value == null) {
            return null;
        }
        switch (value.getType()) {
            case BOOLEAN:
                return ((AdmBooleanNode) value).get();
            case STRING:
                return Boolean.parseBoolean(((AdmStringNode) value).get());
            default:
                throw new CompilationException(ErrorCode.FIELD_NOT_OF_TYPE, getSourceLocation(), optionName,
                        ATypeTag.BOOLEAN, value.getType());
        }
    }

    public Map<String, String> getResources() throws CompilationException {
        IAdmNode value = getOption(RESOURCES_FIELD_NAME);
        if (value == null) {
            return null;
        }
        if (value.getType() != ATypeTag.OBJECT) {
            throw new CompilationException(ErrorCode.FIELD_NOT_OF_TYPE, getSourceLocation(), RESOURCES_FIELD_NAME,
                    ATypeTag.OBJECT, value.getType());
        }
        return ConfigurationUtil.toProperties((AdmObjectNode) value);
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        return visitor.visit(this, arg);
    }

    @Override
    public byte getCategory() {
        return Category.DDL;
    }

    private IAdmNode getOption(String optionName) {
        return options != null ? options.get(optionName) : null;
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
