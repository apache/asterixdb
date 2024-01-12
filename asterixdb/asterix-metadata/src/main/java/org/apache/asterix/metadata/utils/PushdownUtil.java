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
package org.apache.asterix.metadata.utils;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.base.AMissing;
import org.apache.asterix.om.base.ANull;
import org.apache.asterix.om.base.AOrderedList;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.utils.ConstantExpressionUtil;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IAlgebricksConstantValue;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;

public class PushdownUtil {
    //Set of allowed functions that can request a type in its entirety without marking it as leaf (i.e., ANY)
    public static final Set<FunctionIdentifier> YIELDABLE_FUNCTIONS = createYieldableFunctions();
    //Set of supported array functions
    public static final Set<FunctionIdentifier> ARRAY_FUNCTIONS = createSupportedArrayFunctions();
    //Set of supported functions that we can push down (a.k.a. path functions)
    public static final Set<FunctionIdentifier> SUPPORTED_FUNCTIONS = createSupportedFunctions();

    public static final Set<FunctionIdentifier> FILTER_PUSHABLE_PATH_FUNCTIONS = createFilterPushablePathFunctions();
    public static final Set<FunctionIdentifier> COMPARE_FUNCTIONS = createCompareFunctions();
    public static final Set<FunctionIdentifier> RANGE_FILTER_PUSHABLE_FUNCTIONS = createRangeFilterPushableFunctions();
    // Set of aggregate functions in a subplan that allows the SELECT in such subplan to be pushed (SOME and EXISTS)
    public static final Set<FunctionIdentifier> FILTER_PUSHABLE_AGGREGATE_FUNCTIONS =
            createFilterPushableAggregateFunctions();
    public static final Set<FunctionIdentifier> FILTER_PROHIBITED_FUNCTIONS = createFilterProhibitedFunctions();

    private PushdownUtil() {
    }

    public static IVariableTypeEnvironment getTypeEnv(ILogicalOperator useOperator, IOptimizationContext context)
            throws AlgebricksException {
        if (useOperator.getOperatorTag() == LogicalOperatorTag.DATASOURCESCAN
                || useOperator.getOperatorTag() == LogicalOperatorTag.INNERJOIN) {
            // Special case: for pushed select condition
            return useOperator.computeOutputTypeEnvironment(context);
        } else {
            return useOperator.computeInputTypeEnvironment(context);
        }
    }

    public static IVariableTypeEnvironment getTypeEnv(ILogicalOperator useOperator, ILogicalOperator scanOperator,
            IOptimizationContext context) throws AlgebricksException {
        if (useOperator == scanOperator) {
            // Special case: for pushed select condition
            return useOperator.computeOutputTypeEnvironment(context);
        } else {
            return scanOperator.computeOutputTypeEnvironment(context);
        }
    }

    public static String getFieldName(AbstractFunctionCallExpression fieldAccessExpr, IVariableTypeEnvironment typeEnv)
            throws AlgebricksException {
        if (BuiltinFunctions.FIELD_ACCESS_BY_NAME.equals(fieldAccessExpr.getFunctionIdentifier())) {
            return ConstantExpressionUtil.getStringArgument(fieldAccessExpr, 1);
        } else {
            // FIELD_ACCESS_BY_INDEX
            IAType type = (IAType) typeEnv.getType(fieldAccessExpr.getArguments().get(0).getValue());
            ARecordType recordType = getRecordType(type);
            int fieldIdx = ConstantExpressionUtil.getIntArgument(fieldAccessExpr, 1);
            return recordType.getFieldNames()[fieldIdx];
        }
    }

    public static boolean isConstant(ILogicalExpression expression) {
        return expression.getExpressionTag() == LogicalExpressionTag.CONSTANT;
    }

    public static boolean isFilterPath(ILogicalExpression expression) {
        FunctionIdentifier fid = getFunctionIdentifier(expression);
        return fid != null && FILTER_PUSHABLE_PATH_FUNCTIONS.contains(fid);
    }

    public static boolean isCompare(ILogicalExpression expression) {
        FunctionIdentifier fid = getFunctionIdentifier(expression);
        return fid != null && COMPARE_FUNCTIONS.contains(fid);
    }

    public static boolean isAnd(ILogicalExpression expression) {
        FunctionIdentifier fid = getFunctionIdentifier(expression);
        return BuiltinFunctions.AND.equals(fid);
    }

    public static boolean isOr(ILogicalExpression expression) {
        FunctionIdentifier fid = getFunctionIdentifier(expression);
        return BuiltinFunctions.OR.equals(fid);
    }

    public static boolean isNot(ILogicalExpression expression) {
        FunctionIdentifier fid = getFunctionIdentifier(expression);
        return BuiltinFunctions.NOT.equals(fid);
    }

    public static AOrderedList getArrayConstantFromScanCollection(ILogicalExpression expression) {
        FunctionIdentifier fid = getFunctionIdentifier(expression);
        if (!BuiltinFunctions.SCAN_COLLECTION.equals(fid)) {
            return null;
        }

        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expression;
        ILogicalExpression argExpr = funcExpr.getArguments().get(0).getValue();
        if (!isConstant(argExpr)) {
            return null;
        }

        IAObject constValue = getConstant(argExpr);
        return constValue.getType().getTypeTag() == ATypeTag.ARRAY ? (AOrderedList) constValue : null;
    }

    public static boolean isTypeFunction(FunctionIdentifier fid) {
        return fid.getName().startsWith("is");
    }

    public static boolean isNestedFunction(FunctionIdentifier fid) {
        return isObjectFunction(fid) || isArrayOrAggregateFunction(fid) || BuiltinFunctions.DEEP_EQUAL.equals(fid);
    }

    public static boolean isObjectFunction(FunctionIdentifier fid) {
        String functionName = fid.getName();
        return functionName.contains("object") || BuiltinFunctions.PAIRS.equals(fid);
    }

    public static boolean isArrayOrAggregateFunction(FunctionIdentifier fid) {
        String functionName = fid.getName();
        return functionName.startsWith("array") || functionName.startsWith("strict") || functionName.startsWith("sql")
                || BuiltinFunctions.GET_ITEM.equals(fid) || BuiltinFunctions.isBuiltinScalarAggregateFunction(fid)
                || BuiltinFunctions.isBuiltinAggregateFunction(fid);
    }

    public static boolean isSameFunction(ILogicalExpression expr1, ILogicalExpression expr2) {
        FunctionIdentifier fid1 = getFunctionIdentifier(expr1);
        FunctionIdentifier fid2 = getFunctionIdentifier(expr2);
        return fid1 != null && fid1.equals(fid2);
    }

    public static boolean isAllVariableExpressions(List<Mutable<ILogicalExpression>> arguments) {
        return arguments.stream().allMatch(arg -> arg.getValue().getExpressionTag() == LogicalExpressionTag.VARIABLE);
    }

    public static boolean isSupportedFilterAggregateFunction(ILogicalExpression expression) {
        FunctionIdentifier fid = getFunctionIdentifier(expression);
        return fid != null && FILTER_PUSHABLE_AGGREGATE_FUNCTIONS.contains(fid);
    }

    public static boolean isProhibitedFilterFunction(ILogicalExpression expression) {
        FunctionIdentifier fid = getFunctionIdentifier(expression);
        return fid != null && !RANGE_FILTER_PUSHABLE_FUNCTIONS.contains(fid)
                && (isNestedFunction(fid) || isTypeFunction(fid) || FILTER_PROHIBITED_FUNCTIONS.contains(fid));
    }

    public static IAObject getConstant(ILogicalExpression expr) {
        IAlgebricksConstantValue algebricksConstant = ((ConstantExpression) expr).getValue();
        if (algebricksConstant.isTrue()) {
            return ABoolean.TRUE;
        } else if (algebricksConstant.isFalse()) {
            return ABoolean.FALSE;
        } else if (algebricksConstant.isMissing()) {
            return AMissing.MISSING;
        } else if (algebricksConstant.isNull()) {
            return ANull.NULL;
        }

        return ((AsterixConstantValue) algebricksConstant).getObject();
    }

    private static FunctionIdentifier getFunctionIdentifier(ILogicalExpression expression) {
        if (expression.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return null;
        }
        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expression;
        return funcExpr.getFunctionIdentifier();
    }

    private static Set<FunctionIdentifier> createSupportedArrayFunctions() {
        return Set.of(BuiltinFunctions.GET_ITEM, BuiltinFunctions.ARRAY_STAR, BuiltinFunctions.SCAN_COLLECTION);
    }

    private static ARecordType getRecordType(IAType type) {
        IAType recordType = type;
        if (type.getTypeTag() == ATypeTag.UNION) {
            recordType = ((AUnionType) type).getActualType();
        }

        return (ARecordType) recordType;
    }

    private static Set<FunctionIdentifier> createSupportedFunctions() {
        Set<FunctionIdentifier> supportedFunctions = new HashSet<>();
        supportedFunctions.add(BuiltinFunctions.FIELD_ACCESS_BY_NAME);
        supportedFunctions.add(BuiltinFunctions.FIELD_ACCESS_BY_INDEX);
        supportedFunctions.addAll(ARRAY_FUNCTIONS);
        return supportedFunctions;
    }

    private static Set<FunctionIdentifier> createYieldableFunctions() {
        return Set.of(BuiltinFunctions.IS_ARRAY, BuiltinFunctions.IS_MULTISET, BuiltinFunctions.IS_OBJECT,
                BuiltinFunctions.IS_ATOMIC, BuiltinFunctions.IS_BINARY, BuiltinFunctions.IS_POINT,
                BuiltinFunctions.IS_LINE, BuiltinFunctions.IS_RECTANGLE, BuiltinFunctions.IS_CIRCLE,
                BuiltinFunctions.IS_POLYGON, BuiltinFunctions.IS_SPATIAL, BuiltinFunctions.IS_DATE,
                BuiltinFunctions.IS_DATETIME, BuiltinFunctions.IS_TIME, BuiltinFunctions.IS_DURATION,
                BuiltinFunctions.IS_INTERVAL, BuiltinFunctions.IS_TEMPORAL, BuiltinFunctions.IS_UUID,
                BuiltinFunctions.IS_NUMBER, BuiltinFunctions.IS_BOOLEAN, BuiltinFunctions.IS_STRING,
                BuiltinFunctions.IS_SYSTEM_NULL, AlgebricksBuiltinFunctions.IS_MISSING,
                AlgebricksBuiltinFunctions.IS_NULL, BuiltinFunctions.IS_UNKNOWN, BuiltinFunctions.GET_TYPE,
                BuiltinFunctions.SCALAR_SQL_COUNT);
    }

    private static Set<FunctionIdentifier> createFilterPushablePathFunctions() {
        Set<FunctionIdentifier> pushablePathFunctions = new HashSet<>(SUPPORTED_FUNCTIONS);
        // TODO Add support for GET_ITEM.
        pushablePathFunctions.remove(BuiltinFunctions.GET_ITEM);
        return pushablePathFunctions;
    }

    private static Set<FunctionIdentifier> createCompareFunctions() {
        return Set.of(AlgebricksBuiltinFunctions.LE, AlgebricksBuiltinFunctions.GE, AlgebricksBuiltinFunctions.LT,
                AlgebricksBuiltinFunctions.GT, AlgebricksBuiltinFunctions.EQ);
    }

    private static Set<FunctionIdentifier> createRangeFilterPushableFunctions() {
        Set<FunctionIdentifier> pushableFunctions = new HashSet<>(COMPARE_FUNCTIONS);
        pushableFunctions.add(AlgebricksBuiltinFunctions.AND);
        pushableFunctions.add(AlgebricksBuiltinFunctions.OR);
        pushableFunctions.add(AlgebricksBuiltinFunctions.NOT);
        return pushableFunctions;
    }

    private static Set<FunctionIdentifier> createFilterPushableAggregateFunctions() {
        Set<FunctionIdentifier> pushableFunctions = new HashSet<>();
        pushableFunctions.add(BuiltinFunctions.NON_EMPTY_STREAM);
        return pushableFunctions;
    }

    private static Set<FunctionIdentifier> createFilterProhibitedFunctions() {
        Set<FunctionIdentifier> prohibitedFunctions = new HashSet<>();
        prohibitedFunctions.add(BuiltinFunctions.EMPTY_STREAM);
        return prohibitedFunctions;
    }
}
