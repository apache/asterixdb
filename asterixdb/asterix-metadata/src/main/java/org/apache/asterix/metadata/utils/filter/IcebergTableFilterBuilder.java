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
package org.apache.asterix.metadata.utils.filter;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.asterix.common.external.IExternalFilterEvaluatorFactory;
import org.apache.asterix.external.input.filter.IcebergTableFilterEvaluatorFactory;
import org.apache.asterix.om.base.ADate;
import org.apache.asterix.om.base.ADateTime;
import org.apache.asterix.om.base.ADouble;
import org.apache.asterix.om.base.AFloat;
import org.apache.asterix.om.base.AInt16;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.AInt8;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.base.ATime;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.runtime.projection.ExternalDatasetProjectionFiltrationInfo;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class IcebergTableFilterBuilder extends AbstractFilterBuilder {

    private static final Logger LOGGER = LogManager.getLogger();

    public IcebergTableFilterBuilder(ExternalDatasetProjectionFiltrationInfo projectionFiltrationInfo,
            JobGenContext context, IVariableTypeEnvironment typeEnv) {
        super(projectionFiltrationInfo.getFilterPaths(), projectionFiltrationInfo.getFilterExpression(), context,
                typeEnv);
    }

    public IExternalFilterEvaluatorFactory build() throws AlgebricksException {
        Expression icebergTablePredicate = null;
        if (filterExpression != null) {
            try {
                icebergTablePredicate = createIcebergExpression(filterExpression, false);
            } catch (Exception e) {
                LOGGER.warn("Error creating IcebergTable filter expression, skipping filter pushdown", e);
            }
        }
        return new IcebergTableFilterEvaluatorFactory(icebergTablePredicate);
    }

    /**
     * Recursively converts an AsterixDB logical expression into an Iceberg {@link Expression}.
     *
     * @return an Iceberg Expression, or {@code null} if the expression cannot be converted
     */
    protected Expression createIcebergExpression(ILogicalExpression expression, boolean insideNot)
            throws AlgebricksException {
        if (filterPaths.containsKey(expression)) {
            return null;
        } else if (expression.getExpressionTag() == LogicalExpressionTag.CONSTANT) {
            return createBooleanConstantExpression(expression);
        } else if (expression.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
            return handleFunction(expression, insideNot);
        }
        LOGGER.debug("Unsupported expression type {}, skipping filter pushdown", expression);
        return null;
    }

    /**
     * Extracts a Java value from a constant expression for use in comparisons.
     *
     * @return a Java value (String, Number, etc.), or {@code null} for null/missing
     */
    private Object createLiteralValue(ILogicalExpression expression) {
        ConstantExpression constExpr = (ConstantExpression) expression;
        if (constExpr.getValue().isNull() || constExpr.getValue().isMissing()) {
            return null;
        }
        if (!(constExpr.getValue() instanceof AsterixConstantValue)) {
            return null;
        }
        AsterixConstantValue constantValue = (AsterixConstantValue) constExpr.getValue();
        IAObject obj = constantValue.getObject();
        switch (obj.getType().getTypeTag()) {
            case STRING:
                return ((AString) obj).getStringValue();
            case TINYINT:
                return (int) ((AInt8) obj).getByteValue();
            case SMALLINT:
                return (int) ((AInt16) obj).getShortValue();
            case INTEGER:
                return ((AInt32) obj).getIntegerValue();
            case BIGINT:
                return ((AInt64) obj).getLongValue();
            case FLOAT:
                return ((AFloat) obj).getFloatValue();
            case DOUBLE:
                return ((ADouble) obj).getDoubleValue();
            case BOOLEAN:
                return constantValue.isTrue();
            case DATE:
                // Iceberg DATE is represented as days from epoch (int)
                return ((ADate) obj).getChrononTimeInDays();
            case DATETIME:
                // Iceberg TIMESTAMP is represented as microseconds from epoch
                return TimeUnit.MILLISECONDS.toMicros(((ADateTime) obj).getChrononTime());
            case TIME:
                // Iceberg TIME is represented as microseconds from midnight
                return TimeUnit.MILLISECONDS.toMicros(((ATime) obj).getChrononTime());
            default:
                LOGGER.debug("Unsupported literal type: {}", obj.getType());
                return null;
        }
    }

    /**
     * Converts a bare boolean constant to an Iceberg Expression (alwaysTrue / alwaysFalse).
     */
    private Expression createBooleanConstantExpression(ILogicalExpression expression) {
        ConstantExpression constExpr = (ConstantExpression) expression;
        if (constExpr.getValue().isTrue()) {
            return Expressions.alwaysTrue();
        } else if (constExpr.getValue().isFalse()) {
            return Expressions.alwaysFalse();
        }
        // null/missing constant — cannot determine truth value; do not push down
        return null;
    }

    @Override
    protected IScalarEvaluatorFactory createValueAccessor(ILogicalExpression expression) {
        return null;
    }

    private Expression handleFunction(ILogicalExpression expr, boolean insideNot) throws AlgebricksException {
        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expr;
        FunctionIdentifier fid = funcExpr.getFunctionIdentifier();

        // Logical Connectives
        if (fid.equals(AlgebricksBuiltinFunctions.AND)) {
            return handleAnd(funcExpr, insideNot);
        } else if (fid.equals(AlgebricksBuiltinFunctions.OR)) {
            return handleOr(funcExpr, insideNot);
        } else if (fid.equals(AlgebricksBuiltinFunctions.NOT)) {
            return handleNot(funcExpr, insideNot);
        }

        // Null check
        if (fid.equals(AlgebricksBuiltinFunctions.IS_NULL)) {
            return handleIsNull(funcExpr);
        }

        // Comparison operators
        if (fid.equals(AlgebricksBuiltinFunctions.EQ) || fid.equals(AlgebricksBuiltinFunctions.NEQ)
                || fid.equals(AlgebricksBuiltinFunctions.LT) || fid.equals(AlgebricksBuiltinFunctions.LE)
                || fid.equals(AlgebricksBuiltinFunctions.GT) || fid.equals(AlgebricksBuiltinFunctions.GE)) {
            return handleComparison(funcExpr, fid);
        }

        // String functions
        if (fid.equals(BuiltinFunctions.STRING_STARTS_WITH)) {
            return handleStringStartsWith(funcExpr);
        }
        LOGGER.trace("Unsupported function for Iceberg filter pushdown: {}", fid);
        return null;
    }

    private Expression handleAnd(AbstractFunctionCallExpression funcExpr, boolean insideNot)
            throws AlgebricksException {
        Expression result = null;
        for (Mutable<ILogicalExpression> argRef : funcExpr.getArguments()) {
            Expression argExpr = createIcebergExpression(argRef.getValue(), insideNot);
            if (argExpr == null) {
                if (insideNot) {
                    // Under an odd number of NOTs, partial AND pushdown is unsafe.
                    // NOT(AND(e1, e2)) ≡ NOT(e1) OR NOT(e2), so dropping e2 and pushing
                    // NOT(e1) would incorrectly prune rows where e2 is false.
                    return null;
                }
                // Under an even number of NOTs (including zero — top-level or inside OR):
                // skip un-pushable children safely.
                // - Top-level / inside OR at top context: the residual filter in the plan
                //   evaluates skipped conjuncts.
                // - Inside OR: AND(e1, e2) reduced to e1 makes the disjunct weaker, so the
                //   overall OR passes more rows — a safe over-approximation.
                // - Under double-NOT: NOT(NOT(AND(e1,e2))) reduced to NOT(NOT(e1)) = e1,
                //   which is also a safe over-approximation.
                continue;
            }
            result = (result == null) ? argExpr : Expressions.and(result, argExpr);
        }
        return result;
    }

    private Expression handleOr(AbstractFunctionCallExpression funcExpr, boolean insideNot) throws AlgebricksException {
        Expression result = null;
        for (Mutable<ILogicalExpression> argRef : funcExpr.getArguments()) {
            // insideNot (odd-NOT parity) propagates unchanged through OR: OR itself does
            // not introduce or remove negation.
            Expression argExpr = createIcebergExpression(argRef.getValue(), insideNot);
            if (argExpr == null) {
                // One un-pushable disjunct makes the whole OR un-pushable for correctness:
                // skipping a disjunct would over-prune rows matching only that disjunct.
                return null;
            }
            result = (result == null) ? argExpr : Expressions.or(result, argExpr);
        }
        return result;
    }

    private Expression handleNot(AbstractFunctionCallExpression funcExpr, boolean insideNot)
            throws AlgebricksException {
        List<Mutable<ILogicalExpression>> args = funcExpr.getArguments();
        if (args.size() != 1) {
            return null;
        }
        ILogicalExpression innerExpr = args.get(0).getValue();
        // NOT toggles the negation context: each NOT flips whether we are inside an odd
        // or even number of negations.  Partial AND pushdown is only unsafe under an odd
        // number of NOTs (insideNot=true), because:
        //   NOT(AND(e1,e2))  — partial push of e1 yields NOT(e1), which is too strong.
        //   NOT(NOT(AND(e1,e2))) — partial push of e1 yields NOT(NOT(e1)) = e1, which is
        //   a safe over-approximation (passes more rows than the full AND).
        Expression inner = createIcebergExpression(innerExpr, !insideNot);
        return (inner != null) ? Expressions.not(inner) : null;
    }

    private Expression handleIsNull(AbstractFunctionCallExpression funcExpr) throws AlgebricksException {
        List<Mutable<ILogicalExpression>> args = funcExpr.getArguments();
        if (args.size() != 1) {
            return null;
        }
        ILogicalExpression arg = args.get(0).getValue();
        String columnName = tryGetColumnName(arg);
        if (columnName == null) {
            return null;
        }
        return Expressions.isNull(columnName);
    }

    private Expression handleComparison(AbstractFunctionCallExpression funcExpr, FunctionIdentifier fid)
            throws AlgebricksException {
        List<Mutable<ILogicalExpression>> args = funcExpr.getArguments();
        if (args.size() != 2) {
            return null;
        }
        ILogicalExpression left = args.get(0).getValue();
        ILogicalExpression right = args.get(1).getValue();
        String columnName = tryGetColumnName(left);
        Object literalValue;
        boolean flipped = false;
        if (columnName != null) {
            // Normal: column <op> literal
            literalValue = tryGetLiteralValue(right);
        } else {
            // Try flipped: literal <op> column
            columnName = tryGetColumnName(right);
            if (columnName == null) {
                // Neither side is a known column; could be column <op> column — not supported
                return null;
            }
            literalValue = tryGetLiteralValue(left);
            flipped = true;
        }
        if (literalValue == null) {
            return null;
        }
        // When operands are flipped, reverse the comparison direction
        FunctionIdentifier effectiveFid = flipped ? flipComparison(fid) : fid;
        return buildComparisonExpression(effectiveFid, columnName, literalValue);
    }

    private Expression buildComparisonExpression(FunctionIdentifier fid, String columnName, Object value) {
        if (fid.equals(AlgebricksBuiltinFunctions.EQ)) {
            return Expressions.equal(columnName, value);
        } else if (fid.equals(AlgebricksBuiltinFunctions.NEQ)) {
            return Expressions.notEqual(columnName, value);
        } else if (fid.equals(AlgebricksBuiltinFunctions.LT)) {
            return Expressions.lessThan(columnName, value);
        } else if (fid.equals(AlgebricksBuiltinFunctions.LE)) {
            return Expressions.lessThanOrEqual(columnName, value);
        } else if (fid.equals(AlgebricksBuiltinFunctions.GT)) {
            return Expressions.greaterThan(columnName, value);
        } else if (fid.equals(AlgebricksBuiltinFunctions.GE)) {
            return Expressions.greaterThanOrEqual(columnName, value);
        }
        return null;
    }

    /**
     * Flips a binary comparison operator to handle reversed operand order.
     * e.g. {@code literal < column} becomes {@code column > literal}
     */
    private FunctionIdentifier flipComparison(FunctionIdentifier fid) {
        if (fid.equals(AlgebricksBuiltinFunctions.LT)) {
            return AlgebricksBuiltinFunctions.GT;
        } else if (fid.equals(AlgebricksBuiltinFunctions.LE)) {
            return AlgebricksBuiltinFunctions.GE;
        } else if (fid.equals(AlgebricksBuiltinFunctions.GT)) {
            return AlgebricksBuiltinFunctions.LT;
        } else if (fid.equals(AlgebricksBuiltinFunctions.GE)) {
            return AlgebricksBuiltinFunctions.LE;
        }
        // EQ and NEQ are symmetric; return as-is
        return fid;
    }

    private Expression handleStringStartsWith(AbstractFunctionCallExpression funcExpr) throws AlgebricksException {
        List<Mutable<ILogicalExpression>> args = funcExpr.getArguments();
        if (args.size() != 2) {
            return null;
        }
        String columnName = tryGetColumnName(args.get(0).getValue());
        if (columnName == null) {
            return null;
        }
        Object prefix = tryGetLiteralValue(args.get(1).getValue());
        if (!(prefix instanceof String)) {
            return null;
        }
        return Expressions.startsWith(columnName, (String) prefix);
    }

    /**
     * Returns the Iceberg column name for an expression if it refers to a pushed-down filter path,
     * or {@code null} if the expression is not a column reference.
     */
    private String tryGetColumnName(ILogicalExpression expression) {
        if (!filterPaths.containsKey(expression)) {
            return null;
        }
        try {
            return (String) createColumnExpression(expression);
        } catch (Exception e) {
            LOGGER.debug("Failed to create column expression for {}", expression, e);
            return null;
        }
    }

    /**
     * Returns the Java literal value from a constant expression, or {@code null} if the
     * expression is not a constant or the constant type is unsupported.
     */
    private Object tryGetLiteralValue(ILogicalExpression expression) {
        if (expression.getExpressionTag() != LogicalExpressionTag.CONSTANT) {
            return null;
        }
        return createLiteralValue(expression);
    }

    protected Object createColumnExpression(ILogicalExpression expression) {
        ARecordType path = filterPaths.get(expression);
        if (path.getFieldNames().length != 1) {
            throw new RuntimeException("Unsupported column expression: " + expression);
        } else if (path.getFieldTypes()[0].getTypeTag() == ATypeTag.OBJECT) {
            // The field could be a nested field
            List<String> fieldList = new ArrayList<>();
            fieldList = createPathExpression(path, fieldList);
            return String.join(".", fieldList);
        } else if (path.getFieldTypes()[0].getTypeTag() == ATypeTag.ANY) {
            return path.getFieldNames()[0];
        } else {
            throw new RuntimeException("Unsupported column expression: " + expression);
        }
    }

    private List<String> createPathExpression(ARecordType path, List<String> fieldList) {
        if (path.getFieldNames().length != 1) {
            throw new RuntimeException("Error creating column expression");
        } else {
            fieldList.add(path.getFieldNames()[0]);
        }
        if (path.getFieldTypes()[0].getTypeTag() == ATypeTag.OBJECT) {
            return createPathExpression((ARecordType) path.getFieldTypes()[0], fieldList);
        } else if (path.getFieldTypes()[0].getTypeTag() == ATypeTag.ANY) {
            return fieldList;
        } else {
            throw new RuntimeException("Error creating column expression");
        }
    }
}
