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

import java.util.function.BiFunction;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.rewrites.LangRewritingContext;
import org.apache.asterix.lang.common.util.FunctionUtil;
import org.apache.asterix.lang.sqlpp.expression.WindowExpression;
import org.apache.asterix.lang.sqlpp.util.FunctionMapUtil;
import org.apache.asterix.lang.sqlpp.visitor.base.AbstractSqlppSimpleExpressionVisitor;
import org.apache.asterix.om.functions.BuiltinFunctions;

public class SqlppFunctionCallResolverVisitor extends AbstractSqlppSimpleExpressionVisitor {

    private final LangRewritingContext context;

    private final boolean allowNonStoredUdfCalls;

    private final BiFunction<String, Integer, FunctionSignature> builtinFunctionResolver;

    private final BiFunction<String, Integer, FunctionSignature> callExprResolver;

    public SqlppFunctionCallResolverVisitor(LangRewritingContext context, boolean allowNonStoredUdfCalls) {
        this.context = context;
        this.allowNonStoredUdfCalls = allowNonStoredUdfCalls;
        this.builtinFunctionResolver = FunctionUtil.createBuiltinFunctionResolver(context.getMetadataProvider());
        this.callExprResolver = this::resolveCallExpr;
    }

    @Override
    public Expression visit(CallExpr callExpr, ILangExpression arg) throws CompilationException {
        FunctionSignature fs = FunctionUtil.resolveFunctionCall(callExpr.getFunctionSignature(),
                callExpr.getSourceLocation(), context.getMetadataProvider(), callExprResolver, true,
                context.getDeclaredFunctions(), allowNonStoredUdfCalls);
        callExpr.setFunctionSignature(fs);
        return super.visit(callExpr, arg);
    }

    @Override
    public Expression visit(WindowExpression winExpr, ILangExpression arg) throws CompilationException {
        FunctionSignature fs = FunctionUtil.resolveFunctionCall(winExpr.getFunctionSignature(),
                winExpr.getSourceLocation(), context.getMetadataProvider(), callExprResolver, false, null, false);
        winExpr.setFunctionSignature(fs);
        return super.visit(winExpr, arg);
    }

    private FunctionSignature resolveCallExpr(String name, int arity) {
        FunctionSignature fs = builtinFunctionResolver.apply(name, arity);
        if (fs != null) {
            return fs;
        }
        fs = FunctionSignature.newAsterix(name, arity);
        return isAggregateFunction(fs) || isWindowFunction(fs) ? fs : null;
    }

    private static boolean isAggregateFunction(FunctionSignature fs) {
        return FunctionMapUtil.isSql92AggregateFunction(fs) || FunctionMapUtil.isCoreAggregateFunction(fs);
    }

    private static boolean isWindowFunction(FunctionSignature fs) {
        return BuiltinFunctions.getWindowFunction(fs.createFunctionIdentifier()) != null;
    }
}
