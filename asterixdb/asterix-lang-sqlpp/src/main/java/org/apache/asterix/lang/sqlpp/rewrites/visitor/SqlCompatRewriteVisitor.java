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

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.expression.OperatorExpr;
import org.apache.asterix.lang.common.struct.OperatorType;
import org.apache.asterix.lang.sqlpp.clause.Projection;
import org.apache.asterix.lang.sqlpp.visitor.base.AbstractSqlppSimpleExpressionVisitor;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.api.exceptions.SourceLocation;

/**
 * Applies initial rewritings for "SQL-compatible" evaluation mode
 * <ol>
 * <li>Rewrites {@code SELECT *} into {@code SELECT *.*}
 * <li>Rewrites {@code NOT? IN expr} into {@code NOT? IN to_array(expr)} if {@code expr} can return a non-list
 * </ol>
 */
public final class SqlCompatRewriteVisitor extends AbstractSqlppSimpleExpressionVisitor {

    @Override
    public Expression visit(Projection projection, ILangExpression arg) throws CompilationException {
        if (projection.getKind() == Projection.Kind.STAR) {
            projection.setKind(Projection.Kind.EVERY_VAR_STAR);
        }
        return super.visit(projection, arg);
    }

    @Override
    public Expression visit(OperatorExpr opExpr, ILangExpression arg) throws CompilationException {
        List<OperatorType> opTypeList = opExpr.getOpList();
        if (opTypeList.size() == 1) {
            switch (opTypeList.get(0)) {
                case IN:
                case NOT_IN:
                    List<Expression> exprList = opExpr.getExprList();
                    Expression arg1 = exprList.get(1);
                    if (!alwaysReturnsList(arg1)) {
                        List<Expression> newExprList = new ArrayList<>(2);
                        newExprList.add(exprList.get(0));
                        newExprList.add(createCallExpr(BuiltinFunctions.TO_ARRAY, arg1, opExpr.getSourceLocation()));
                        opExpr.setExprList(newExprList);
                    }
                    break;
            }
        }
        return super.visit(opExpr, arg);
    }

    private static boolean alwaysReturnsList(Expression expr) {
        switch (expr.getKind()) {
            case LIST_CONSTRUCTOR_EXPRESSION:
            case LIST_SLICE_EXPRESSION:
            case SELECT_EXPRESSION:
                return true;
            default:
                return false;
        }
    }

    private static CallExpr createCallExpr(FunctionIdentifier fid, Expression inExpr, SourceLocation sourceLoc) {
        List<Expression> argList = new ArrayList<>(1);
        argList.add(inExpr);
        CallExpr callExpr = new CallExpr(new FunctionSignature(fid), argList);
        callExpr.setSourceLocation(sourceLoc);
        return callExpr;
    }
}
