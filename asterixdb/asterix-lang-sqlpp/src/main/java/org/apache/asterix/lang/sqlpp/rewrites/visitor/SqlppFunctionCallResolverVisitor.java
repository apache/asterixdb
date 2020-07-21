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
package org.apache.asterix.lang.sqlpp.rewrites.visitor;

import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.functions.FunctionConstants;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.statement.FunctionDecl;
import org.apache.asterix.lang.common.util.FunctionUtil;
import org.apache.asterix.lang.sqlpp.expression.WindowExpression;
import org.apache.asterix.lang.sqlpp.util.FunctionMapUtil;
import org.apache.asterix.lang.sqlpp.visitor.base.AbstractSqlppSimpleExpressionVisitor;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.om.functions.BuiltinFunctions;

public final class SqlppFunctionCallResolverVisitor extends AbstractSqlppSimpleExpressionVisitor {

    private final MetadataProvider metadataProvider;

    private final Set<FunctionSignature> declaredFunctions;

    private final BiFunction<String, Integer, FunctionSignature> builtinFunctionResolver;

    private final BiFunction<String, Integer, FunctionSignature> callExprResolver;

    public SqlppFunctionCallResolverVisitor(MetadataProvider metadataProvider, List<FunctionDecl> declaredFunctions) {
        this.metadataProvider = metadataProvider;
        this.declaredFunctions = FunctionUtil.getFunctionSignatures(declaredFunctions);
        this.builtinFunctionResolver = FunctionUtil.createBuiltinFunctionResolver(metadataProvider);
        this.callExprResolver = this::resolveCallExpr;
    }

    @Override
    public Expression visit(CallExpr callExpr, ILangExpression arg) throws CompilationException {
        FunctionSignature fs = FunctionUtil.resolveFunctionCall(callExpr.getFunctionSignature(),
                callExpr.getSourceLocation(), metadataProvider, declaredFunctions, callExprResolver);
        callExpr.setFunctionSignature(fs);
        return super.visit(callExpr, arg);
    }

    @Override
    public Expression visit(WindowExpression winExpr, ILangExpression arg) throws CompilationException {
        FunctionSignature fs = FunctionUtil.resolveFunctionCall(winExpr.getFunctionSignature(),
                winExpr.getSourceLocation(), metadataProvider, declaredFunctions, callExprResolver);
        winExpr.setFunctionSignature(fs);
        return super.visit(winExpr, arg);
    }

    private FunctionSignature resolveCallExpr(String name, int arity) {
        FunctionSignature fs = builtinFunctionResolver.apply(name, arity);
        if (fs != null) {
            return fs;
        }
        fs = new FunctionSignature(FunctionConstants.ASTERIX_DV, name, arity);
        return isAggregateFunction(fs) || isWindowFunction(fs) ? fs : null;
    }

    private static boolean isAggregateFunction(FunctionSignature fs) {
        return FunctionMapUtil.isSql92AggregateFunction(fs) || FunctionMapUtil.isCoreAggregateFunction(fs);
    }

    private static boolean isWindowFunction(FunctionSignature fs) {
        return BuiltinFunctions.getWindowFunction(fs.createFunctionIdentifier()) != null;
    }
}