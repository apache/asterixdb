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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.util.ExpressionUtils;
import org.apache.asterix.lang.common.util.VectorDistanceMetric;
import org.apache.asterix.lang.sqlpp.visitor.base.AbstractSqlppSimpleExpressionVisitor;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.hyracks.util.annotations.AiProvenance;

/**
 * Rewrites {@code vector_distance(vec1, vec2, metric)} into the corresponding 2-arg vector builtin when
 * {@code metric} is a compile-time string literal.
 */
@AiProvenance(agent = AiProvenance.Agent.GPT_5_3, tool = AiProvenance.Tool.CURSOR, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "Compile-time rewrite for vector_distance sugar")
public final class SqlppVectorDistanceRewriteVisitor extends AbstractSqlppSimpleExpressionVisitor {

    @Override
    public Expression visit(CallExpr callExpr, ILangExpression arg) throws CompilationException {
        List<Expression> rewrittenArgs = new ArrayList<>(callExpr.getExprList().size());
        for (Expression expr : callExpr.getExprList()) {
            rewrittenArgs.add(expr.accept(this, arg));
        }
        callExpr.setExprList(rewrittenArgs);
        if (isVectorDistanceCall(callExpr)) {
            return rewriteVectorDistance(callExpr);
        }
        return callExpr;
    }

    private static boolean isVectorDistanceCall(CallExpr callExpr) {
        FunctionSignature fs = callExpr.getFunctionSignature();
        return fs != null && BuiltinFunctions.VECTOR_DISTANCE.getArity() == fs.getArity()
                && BuiltinFunctions.VECTOR_DISTANCE.getName().equals(fs.getName());
    }

    private static CallExpr rewriteVectorDistance(CallExpr callExpr) throws CompilationException {
        List<Expression> args = callExpr.getExprList();
        if (args.size() != BuiltinFunctions.VECTOR_DISTANCE.getArity()) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, callExpr.getSourceLocation(),
                    "vector_distance expects " + BuiltinFunctions.VECTOR_DISTANCE.getArity() + " arguments");
        }
        String metric = ExpressionUtils.getStringLiteral(args.get(2));
        if (metric == null) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, args.get(2).getSourceLocation(),
                    "vector_distance metric must be a compile-time string literal");
        }
        Optional<String> builtinName = VectorDistanceMetric.resolve(metric);
        if (builtinName.isEmpty()) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, args.get(2).getSourceLocation(),
                    "unknown vector_distance metric '" + metric + "'; supported metrics: "
                            + VectorDistanceMetric.supportedMetricsMessage());
        }
        List<Expression> targetArgs = new ArrayList<>(2);
        targetArgs.add(args.get(0));
        targetArgs.add(args.get(1));
        CallExpr rewritten = new CallExpr(FunctionSignature.newAsterix(builtinName.get(), 2), targetArgs,
                callExpr.getAggregateFilterExpr());
        rewritten.setSourceLocation(callExpr.getSourceLocation());
        return rewritten;
    }
}
