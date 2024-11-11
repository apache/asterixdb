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

import static org.apache.asterix.common.utils.ConstantUtil.LIKE_ESCAPE;
import static org.apache.asterix.common.utils.ConstantUtil.PERCENT;
import static org.apache.asterix.common.utils.ConstantUtil.UNDERSCORE;
import static org.apache.asterix.lang.sqlpp.rewrites.visitor.OperatorExpressionVisitor.LikePattern.EQUAL;
import static org.apache.asterix.lang.sqlpp.rewrites.visitor.OperatorExpressionVisitor.LikePattern.PREFIX;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.expression.OperatorExpr;
import org.apache.asterix.lang.common.expression.QuantifiedExpression;
import org.apache.asterix.lang.common.expression.QuantifiedExpression.Quantifier;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.literal.FalseLiteral;
import org.apache.asterix.lang.common.literal.StringLiteral;
import org.apache.asterix.lang.common.literal.TrueLiteral;
import org.apache.asterix.lang.common.rewrites.LangRewritingContext;
import org.apache.asterix.lang.common.struct.OperatorType;
import org.apache.asterix.lang.common.struct.QuantifiedPair;
import org.apache.asterix.lang.common.util.ExpressionUtils;
import org.apache.asterix.lang.sqlpp.expression.CaseExpression;
import org.apache.asterix.lang.sqlpp.util.FunctionMapUtil;
import org.apache.asterix.lang.sqlpp.util.SqlppRewriteUtil;
import org.apache.asterix.lang.sqlpp.visitor.base.AbstractSqlppExpressionScopingVisitor;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.expressions.IExpressionAnnotation;
import org.apache.hyracks.algebricks.core.algebra.expressions.PredicateCardinalityAnnotation;
import org.apache.hyracks.api.exceptions.SourceLocation;

public class OperatorExpressionVisitor extends AbstractSqlppExpressionScopingVisitor {

    public OperatorExpressionVisitor(LangRewritingContext context) {
        super(context);
    }

    @Override
    public Expression visit(OperatorExpr operatorExpr, ILangExpression arg) throws CompilationException {
        List<Expression> newExprList = new ArrayList<>();
        for (Expression expr : operatorExpr.getExprList()) {
            newExprList.add(expr.accept(this, operatorExpr));
        }
        operatorExpr.setExprList(newExprList);
        OperatorType opType = operatorExpr.getOpList().get(0);
        switch (opType) {
            // There can only be one LIKE/NOT_LIKE/IN/NOT_IN in an operator expression (according to the grammar).
            case LIKE:
            case NOT_LIKE:
                return processLikeOperator(operatorExpr, opType);
            case IN:
            case NOT_IN:
                return processInOperator(operatorExpr, opType);
            case CONCAT:
                // There can be multiple "||"s in one operator expression (according to the grammar).
                return processConcatOperator(operatorExpr);
            case BETWEEN:
            case NOT_BETWEEN:
                return processBetweenOperator(operatorExpr, opType);
            case DISTINCT:
            case NOT_DISTINCT:
                return processDistinctOperator(operatorExpr, opType);
        }
        return operatorExpr;
    }

    private Expression processLikeOperator(OperatorExpr operatorExpr, OperatorType opType) throws CompilationException {
        switch (opType) {
            case LIKE:
                Expression target = operatorExpr.getExprList().get(0);
                Expression patternExpr = operatorExpr.getExprList().get(1);
                String patternStr = ExpressionUtils.getStringLiteral(patternExpr);
                if (patternStr != null) {
                    StringBuilder likePatternStr = new StringBuilder();
                    LikePattern likePattern = processPattern(patternStr, likePatternStr);
                    if (likePattern == PREFIX) {
                        return convertLikeToRange(operatorExpr, target, likePatternStr.toString());
                    } else if (likePattern == EQUAL) {
                        Expression processedExpr = new LiteralExpr(new StringLiteral(likePatternStr.toString()));
                        return createOperatorExpression(OperatorType.EQ, target, processedExpr, operatorExpr.getHints(),
                                operatorExpr.getSourceLocation());
                    }
                }
                return createLikeExpression(operatorExpr);
            case NOT_LIKE:
                CallExpr notLikeExpr = new CallExpr(new FunctionSignature(BuiltinFunctions.NOT),
                        new ArrayList<>(Collections.singletonList(createLikeExpression(operatorExpr))));
                notLikeExpr.setSourceLocation(operatorExpr.getSourceLocation());
                return notLikeExpr;
            default:
                throw new IllegalArgumentException(String.valueOf(opType));
        }
    }

    private Expression processInOperator(OperatorExpr operatorExpr, OperatorType opType) {
        VariableExpr bindingVar = new VariableExpr(context.newVariable());
        bindingVar.setSourceLocation(operatorExpr.getSourceLocation());
        Expression itemExpr = operatorExpr.getExprList().get(0);
        Expression collectionExpr = operatorExpr.getExprList().get(1);
        OperatorExpr comparison = new OperatorExpr();
        comparison.addOperand(itemExpr);
        comparison.addOperand(bindingVar);
        comparison.setCurrentop(true);
        comparison.addHints(operatorExpr.getHints());
        comparison.setSourceLocation(operatorExpr.getSourceLocation());
        if (opType == OperatorType.IN) {
            comparison.addOperator(OperatorType.EQ);
            QuantifiedExpression quantExpr = new QuantifiedExpression(Quantifier.SOME,
                    new ArrayList<>(Collections.singletonList(new QuantifiedPair(bindingVar, collectionExpr))),
                    comparison);
            quantExpr.setSourceLocation(operatorExpr.getSourceLocation());
            return quantExpr;
        } else {
            comparison.addOperator(OperatorType.NEQ);
            QuantifiedExpression quantExpr = new QuantifiedExpression(Quantifier.EVERY,
                    new ArrayList<>(Collections.singletonList(new QuantifiedPair(bindingVar, collectionExpr))),
                    comparison);
            quantExpr.setSourceLocation(operatorExpr.getSourceLocation());
            return quantExpr;
        }
    }

    private Expression processConcatOperator(OperatorExpr operatorExpr) {
        // All operators have to be "||"s (according to the grammar).
        CallExpr callExpr =
                new CallExpr(FunctionSignature.newAsterix(FunctionMapUtil.CONCAT, 1), operatorExpr.getExprList());
        callExpr.setSourceLocation(operatorExpr.getSourceLocation());
        return callExpr;
    }

    private List<IExpressionAnnotation> removeSelectivityHints(OperatorExpr expr) {
        if (expr.hasHints()) {
            List<IExpressionAnnotation> copyHintsExceptSelectivity = new ArrayList<>();
            for (IExpressionAnnotation h : expr.getHints()) {
                if (!(h.getClass().equals(PredicateCardinalityAnnotation.class))) {
                    copyHintsExceptSelectivity.add(h);
                }
            }
            return copyHintsExceptSelectivity;
        } else {
            return expr.getHints();
        }
    }

    private Expression processBetweenOperator(OperatorExpr operatorExpr, OperatorType opType)
            throws CompilationException {
        // The grammar guarantees that the BETWEEN operator gets exactly three expressions.
        Expression target = operatorExpr.getExprList().get(0);
        Expression left = operatorExpr.getExprList().get(1);
        Expression right = operatorExpr.getExprList().get(2);
        Expression andExpr = createRangeExpression(target, OperatorType.GE, left, right, OperatorType.LE, operatorExpr);

        switch (opType) {
            case BETWEEN:
                return andExpr;
            case NOT_BETWEEN:
                CallExpr callExpr = new CallExpr(new FunctionSignature(BuiltinFunctions.NOT),
                        new ArrayList<>(Collections.singletonList(andExpr)));
                callExpr.setSourceLocation(operatorExpr.getSourceLocation());
                return callExpr;
            default:
                throw new IllegalArgumentException(String.valueOf(opType));
        }
    }

    private Expression processDistinctOperator(OperatorExpr operatorExpr, OperatorType opType)
            throws CompilationException {

        // lhs IS NOT DISTINCT FROM rhs =>
        //   CASE
        //      WHEN (lhs = rhs) OR (lhs IS NULL AND rhs IS NULL) OR (lhs IS MISSING AND rhs IS MISSING)
        //      THEN TRUE
        //      ELSE FALSE
        //   END
        //
        // lhs IS DISTINCT FROM rhs => NOT ( lhs IS NOT DISTINCT FROM rhs )

        Expression lhs = operatorExpr.getExprList().get(0);
        Expression rhs = operatorExpr.getExprList().get(1);

        Expression lhsEqRhs = createOperatorExpression(OperatorType.EQ, lhs, rhs, operatorExpr.getHints(),
                operatorExpr.getSourceLocation());

        CallExpr lhsIsNull = new CallExpr(new FunctionSignature(BuiltinFunctions.IS_NULL),
                new ArrayList<>(Collections.singletonList((Expression) SqlppRewriteUtil.deepCopy(lhs))));
        lhsIsNull.setSourceLocation(operatorExpr.getSourceLocation());

        CallExpr rhsIsNull = new CallExpr(new FunctionSignature(BuiltinFunctions.IS_NULL),
                new ArrayList<>(Collections.singletonList((Expression) SqlppRewriteUtil.deepCopy(rhs))));
        rhsIsNull.setSourceLocation(operatorExpr.getSourceLocation());

        CallExpr lhsIsMissing = new CallExpr(new FunctionSignature(BuiltinFunctions.IS_MISSING),
                new ArrayList<>(Collections.singletonList((Expression) SqlppRewriteUtil.deepCopy(lhs))));
        lhsIsMissing.setSourceLocation(operatorExpr.getSourceLocation());

        CallExpr rhsIsMissing = new CallExpr(new FunctionSignature(BuiltinFunctions.IS_MISSING),
                new ArrayList<>(Collections.singletonList((Expression) SqlppRewriteUtil.deepCopy(rhs))));
        rhsIsMissing.setSourceLocation(operatorExpr.getSourceLocation());

        Expression bothAreNull = createOperatorExpression(OperatorType.AND, lhsIsNull, rhsIsNull, null,
                operatorExpr.getSourceLocation());

        Expression bothAreMissing = createOperatorExpression(OperatorType.AND, lhsIsMissing, rhsIsMissing, null,
                operatorExpr.getSourceLocation());

        Expression bothAreNullOrMissing = createOperatorExpression(OperatorType.OR, bothAreNull, bothAreMissing, null,
                operatorExpr.getSourceLocation());

        Expression eqOrNullOrMissing = createOperatorExpression(OperatorType.OR, lhsEqRhs, bothAreNullOrMissing, null,
                operatorExpr.getSourceLocation());

        CaseExpression caseExpr = new CaseExpression(new LiteralExpr(TrueLiteral.INSTANCE),
                new ArrayList<>(Collections.singletonList(eqOrNullOrMissing)),
                new ArrayList<>(Collections.singletonList(new LiteralExpr(TrueLiteral.INSTANCE))),
                new LiteralExpr(FalseLiteral.INSTANCE));
        caseExpr.setSourceLocation(operatorExpr.getSourceLocation());

        switch (opType) {
            case NOT_DISTINCT:
                return caseExpr;
            case DISTINCT:
                CallExpr callExpr = new CallExpr(new FunctionSignature(BuiltinFunctions.NOT),
                        new ArrayList<>(Collections.singletonList(caseExpr)));
                callExpr.setSourceLocation(operatorExpr.getSourceLocation());
                return callExpr;
            default:
                throw new IllegalArgumentException(String.valueOf(opType));
        }
    }

    private Expression createOperatorExpression(OperatorType opType, Expression lhs, Expression rhs,
            List<IExpressionAnnotation> hints, SourceLocation sourceLoc) {
        OperatorExpr comparison = new OperatorExpr();
        comparison.addOperand(lhs);
        comparison.addOperand(rhs);
        comparison.addOperator(opType);
        comparison.setSourceLocation(sourceLoc);
        if (hints != null) {
            for (IExpressionAnnotation hint : hints) {
                comparison.addHint(hint);
            }
        }
        return comparison;
    }

    private Expression createRangeExpression(Expression target, OperatorType leftOp, Expression left, Expression right,
            OperatorType rightOp, OperatorExpr operatorExpr) throws CompilationException {
        Expression leftComparison = createOperatorExpression(leftOp, target, left, operatorExpr.getHints(),
                operatorExpr.getSourceLocation());
        Expression targetCopy = (Expression) SqlppRewriteUtil.deepCopy(target);

        // remove any selectivity hints from operatorExpr; do not want to duplicate those hints
        Expression rightComparison = createOperatorExpression(rightOp, targetCopy, right,
                removeSelectivityHints(operatorExpr), operatorExpr.getSourceLocation());

        Expression andExpr = createOperatorExpression(OperatorType.AND, leftComparison, rightComparison, null,
                operatorExpr.getSourceLocation());
        return andExpr;
    }

    private Expression convertLikeToRange(OperatorExpr operatorExpr, Expression target, String prefix)
            throws CompilationException {
        int lastCodePoint = prefix.codePointAt(prefix.length() - 1);
        String incrementedLastChar = new String(Character.toChars(lastCodePoint + 1));
        String incrementedStr = prefix.substring(0, prefix.length() - 1) + incrementedLastChar;
        Expression left = new LiteralExpr(new StringLiteral(prefix));
        Expression right = new LiteralExpr(new StringLiteral(incrementedStr));
        return createRangeExpression(target, OperatorType.GE, left, right, OperatorType.LT, operatorExpr);
    }

    private static CallExpr createLikeExpression(OperatorExpr operatorExpr) {
        CallExpr likeExpr =
                new CallExpr(new FunctionSignature(BuiltinFunctions.STRING_LIKE), operatorExpr.getExprList());
        likeExpr.addHints(operatorExpr.getHints());
        likeExpr.setSourceLocation(operatorExpr.getSourceLocation());
        return likeExpr;
    }

    private static LikePattern processPattern(String pattern, StringBuilder likePatternStr) {
        // note: similar logic is applied in StringLikeDescriptor
        LikePattern likePattern = EQUAL;
        for (int i = 0, length = pattern.length(); i < length; i++) {
            char c = pattern.charAt(i);
            if (c == LIKE_ESCAPE) {
                char nextChar;
                // escape character can't be last, and only %, _ and the escape char are allowed after it
                if (i >= length - 1 || ((nextChar = pattern.charAt(i + 1)) != PERCENT && nextChar != UNDERSCORE
                        && nextChar != LIKE_ESCAPE)) {
                    return null;
                }
                likePatternStr.append(nextChar);
                ++i;
            } else if (c == PERCENT && i == pattern.length() - 1) {
                likePattern = PREFIX;
            } else if (c == UNDERSCORE || c == PERCENT) {
                return null;
            } else {
                likePatternStr.append(c);
            }
        }
        return likePattern;
    }

    enum LikePattern {
        PREFIX,
        EQUAL
    }
}
