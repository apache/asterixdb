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

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.functions.FunctionConstants;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.sqlpp.expression.WindowExpression;
import org.apache.asterix.lang.sqlpp.util.FunctionMapUtil;
import org.apache.asterix.lang.sqlpp.visitor.base.AbstractSqlppSimpleExpressionVisitor;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.api.exceptions.SourceLocation;

public final class SqlppSpecialFunctionNameRewriteVisitor extends AbstractSqlppSimpleExpressionVisitor {

    @Override
    public Expression visit(CallExpr callExpr, ILangExpression arg) throws CompilationException {
        callExpr.setFunctionSignature(
                rewriteCoreAggregateFunction(callExpr.getFunctionSignature(), callExpr.getSourceLocation()));
        return super.visit(callExpr, arg);
    }

    @Override
    public Expression visit(WindowExpression winExpr, ILangExpression arg) throws CompilationException {
        winExpr.setFunctionSignature(
                rewriteCoreAggregateFunction(winExpr.getFunctionSignature(), winExpr.getSourceLocation()));
        return super.visit(winExpr, arg);
    }

    private static FunctionSignature rewriteCoreAggregateFunction(FunctionSignature fs, SourceLocation sourceLoc)
            throws CompilationException {
        FunctionIdentifier coreAggregate = FunctionMapUtil.findInternalCoreAggregateFunction(fs);
        if (coreAggregate != null) {
            return new FunctionSignature(FunctionConstants.ASTERIX_DV, coreAggregate.getName(), fs.getArity());
        }
        if (FunctionMapUtil.isSql92AggregateFunction(fs)) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                    fs.getName() + " is a SQL-92 aggregate function. The SQL++ core aggregate function "
                            + FunctionMapUtil.sql92ToCoreAggregateFunction(fs).getName()
                            + " could potentially express the intent.");
        }
        if (FunctionMapUtil.getInternalWindowFunction(fs) != null) {
            throw new CompilationException(ErrorCode.COMPILATION_UNEXPECTED_WINDOW_EXPRESSION, sourceLoc);
        }
        return fs;
    }
}
