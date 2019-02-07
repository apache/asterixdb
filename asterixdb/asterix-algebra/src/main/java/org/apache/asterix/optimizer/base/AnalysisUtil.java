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
package org.apache.asterix.optimizer.base;

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.metadata.declared.DataSourceId;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.optimizer.rules.am.AccessMethodUtils;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractDataSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.WindowOperator;

public class AnalysisUtil {
    /*
     * If the first child of op is of type opType, then it returns that child,
     * o/w returns null.
     */
    public final static ILogicalOperator firstChildOfType(AbstractLogicalOperator op, LogicalOperatorTag opType) {
        List<Mutable<ILogicalOperator>> ins = op.getInputs();
        if (ins == null || ins.isEmpty()) {
            return null;
        }
        Mutable<ILogicalOperator> opRef2 = ins.get(0);
        AbstractLogicalOperator op2 = (AbstractLogicalOperator) opRef2.getValue();
        if (op2.getOperatorTag() == opType) {
            return op2;
        } else {
            return null;
        }
    }

    public static int numberOfVarsInExpr(ILogicalExpression e) {
        switch (e.getExpressionTag()) {
            case CONSTANT: {
                return 0;
            }
            case FUNCTION_CALL: {
                AbstractFunctionCallExpression f = (AbstractFunctionCallExpression) e;
                int s = 0;
                for (Mutable<ILogicalExpression> arg : f.getArguments()) {
                    s += numberOfVarsInExpr(arg.getValue());
                }
                return s;
            }
            case VARIABLE: {
                return 1;
            }
            default: {
                assert false;
                throw new IllegalArgumentException();
            }
        }
    }

    public static boolean isRunnableFieldAccessFunction(FunctionIdentifier fid) {
        return fieldAccessFunctions.contains(fid);
    }

    public static boolean isRunnableAccessToFieldRecord(ILogicalExpression expr) {
        if (expr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
            AbstractFunctionCallExpression fc = (AbstractFunctionCallExpression) expr;
            FunctionIdentifier fid = fc.getFunctionIdentifier();
            if (AnalysisUtil.isRunnableFieldAccessFunction(fid)) {
                return true;
            }
        }
        return false;
    }

    public static boolean isAccessByNameToFieldRecord(ILogicalExpression expr) {
        if (expr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
            AbstractFunctionCallExpression fc = (AbstractFunctionCallExpression) expr;
            FunctionIdentifier fid = fc.getFunctionIdentifier();
            if (fid.equals(BuiltinFunctions.FIELD_ACCESS_BY_NAME)) {
                return true;
            }
        }
        return false;
    }

    public static boolean isAccessToFieldRecord(ILogicalExpression expr) {
        if (expr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
            AbstractFunctionCallExpression fc = (AbstractFunctionCallExpression) expr;
            FunctionIdentifier fid = fc.getFunctionIdentifier();
            if (fid.equals(BuiltinFunctions.FIELD_ACCESS_BY_INDEX) || fid.equals(BuiltinFunctions.FIELD_ACCESS_BY_NAME)
                    || fid.equals(BuiltinFunctions.FIELD_ACCESS_NESTED)) {
                return true;
            }
        }
        return false;
    }

    public static Pair<String, String> getDatasetInfo(AbstractDataSourceOperator op) throws AlgebricksException {
        DataSourceId srcId = (DataSourceId) op.getDataSource().getId();
        return new Pair<>(srcId.getDataverseName(), srcId.getDatasourceName());
    }

    public static Pair<String, String> getExternalDatasetInfo(UnnestMapOperator op) throws AlgebricksException {
        AbstractFunctionCallExpression unnestExpr = (AbstractFunctionCallExpression) op.getExpressionRef().getValue();
        String dataverseName = AccessMethodUtils.getStringConstant(unnestExpr.getArguments().get(0));
        String datasetName = AccessMethodUtils.getStringConstant(unnestExpr.getArguments().get(1));
        return new Pair<>(dataverseName, datasetName);
    }

    /**
     * Checks whether a window operator has a function call where the function has given property
     */
    public static boolean hasFunctionWithProperty(WindowOperator winOp,
            BuiltinFunctions.WindowFunctionProperty property) throws CompilationException {
        for (Mutable<ILogicalExpression> exprRef : winOp.getExpressions()) {
            ILogicalExpression expr = exprRef.getValue();
            if (expr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
                throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, winOp.getSourceLocation(),
                        expr.getExpressionTag());
            }
            AbstractFunctionCallExpression callExpr = (AbstractFunctionCallExpression) expr;
            if (BuiltinFunctions.builtinFunctionHasProperty(callExpr.getFunctionIdentifier(), property)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Checks whether frame boundary expression is a monotonically non-descreasing function over a frame value variable
     */
    public static boolean isWindowFrameBoundaryMonotonic(List<Mutable<ILogicalExpression>> frameBoundaryExprList,
            List<Pair<OrderOperator.IOrder, Mutable<ILogicalExpression>>> frameValueExprList) {
        if (frameValueExprList.size() != 1) {
            return false;
        }
        ILogicalExpression frameValueExpr = frameValueExprList.get(0).second.getValue();
        if (frameValueExpr.getExpressionTag() != LogicalExpressionTag.VARIABLE) {
            return false;
        }
        if (frameBoundaryExprList.size() != 1) {
            return false;
        }
        ILogicalExpression frameStartExpr = frameBoundaryExprList.get(0).getValue();
        switch (frameStartExpr.getExpressionTag()) {
            case CONSTANT:
                return true;
            case VARIABLE:
                return frameStartExpr.equals(frameValueExpr);
            case FUNCTION_CALL:
                AbstractFunctionCallExpression frameStartCallExpr = (AbstractFunctionCallExpression) frameStartExpr;
                FunctionIdentifier fi = frameStartCallExpr.getFunctionIdentifier();
                return (BuiltinFunctions.NUMERIC_ADD.equals(fi) || BuiltinFunctions.NUMERIC_SUBTRACT.equals(fi))
                        && frameStartCallExpr.getArguments().get(0).getValue().equals(frameValueExpr)
                        && frameStartCallExpr.getArguments().get(1).getValue()
                                .getExpressionTag() == LogicalExpressionTag.CONSTANT;
            default:
                throw new IllegalStateException(String.valueOf(frameStartExpr.getExpressionTag()));
        }
    }

    public static boolean isTrivialAggregateSubplan(ILogicalPlan subplan) {
        if (subplan.getRoots().isEmpty()) {
            return false;
        }
        for (Mutable<ILogicalOperator> rootOpRef : subplan.getRoots()) {
            ILogicalOperator rootOp = rootOpRef.getValue();
            if (rootOp.getOperatorTag() != LogicalOperatorTag.AGGREGATE) {
                return false;
            }
            if (firstChildOfType((AbstractLogicalOperator) rootOp, LogicalOperatorTag.NESTEDTUPLESOURCE) == null) {
                return false;
            }
        }
        return true;
    }

    private static List<FunctionIdentifier> fieldAccessFunctions = new ArrayList<>();

    static {
        fieldAccessFunctions.add(BuiltinFunctions.GET_DATA);
        fieldAccessFunctions.add(BuiltinFunctions.GET_HANDLE);
        fieldAccessFunctions.add(BuiltinFunctions.TYPE_OF);
    }
}
