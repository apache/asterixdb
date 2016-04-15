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
import java.util.Collection;
import java.util.List;

import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractDataSourceOperator;
import org.apache.asterix.metadata.declared.AqlSourceId;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.optimizer.rules.am.AccessMethodUtils;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression.FunctionKind;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.SortMergeJoinExpressionAnnotation.SortMergeJoinType;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractLogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions.ComparisonKind;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestMapOperator;
import org.apache.hyracks.algebricks.core.config.AlgebricksConfig;

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
        switch (((AbstractLogicalExpression) e).getExpressionTag()) {
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

    private static SortMergeJoinType getSortMergeJoinable(ILogicalExpression e, Collection<LogicalVariable> inLeftAll,
            Collection<LogicalVariable> inRightAll, List<LogicalVariable> outLeftFields,
            List<LogicalVariable> outRightFields, List<Pair<ILogicalExpression, ILogicalExpression>> outBandRanges) {
        AbstractFunctionCallExpression fexp = (AbstractFunctionCallExpression) e;
        FunctionIdentifier fi = fexp.getFunctionIdentifier();
        if (fi.equals(AlgebricksBuiltinFunctions.AND)) {
            SortMergeJoinType retType = SortMergeJoinType.NESTLOOP;
            for (Mutable<ILogicalExpression> a : fexp.getArguments()) {
                SortMergeJoinType childType = getSortMergeJoinable(a.getValue(), inLeftAll, inRightAll, outLeftFields,
                        outRightFields, outBandRanges);
                if (SortMergeJoinType.BAND == childType)
                    retType = SortMergeJoinType.BAND;
                else if (retType != SortMergeJoinType.BAND && SortMergeJoinType.THETA == childType)
                    retType = SortMergeJoinType.THETA;
                // else if ...
                // For Metric and Skyline join type in the future.
            }
            return retType;
        } else {
            ComparisonKind ck = AlgebricksBuiltinFunctions.getComparisonType(fi);
            if (null == ck || ck == ComparisonKind.EQ) {
                AlgebricksConfig.ALGEBRICKS_LOGGER
                        .info("// SortMerge joinable encounter equal or fj condition -- Condition for" + e + ": " + ck);
                return null;
            }
            ILogicalExpression opLeft = fexp.getArguments().get(0).getValue();
            ILogicalExpression opRight = fexp.getArguments().get(1).getValue();
            if (opLeft.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL
                    && opRight.getExpressionTag() == LogicalExpressionTag.CONSTANT) {
                ScalarFunctionCallExpression sfe = (ScalarFunctionCallExpression) opLeft;
                if (FunctionKind.SCALAR != sfe.getKind()
                        && AsterixBuiltinFunctions.NUMERIC_SUBTRACT != sfe.getFunctionIdentifier())
                    return null;
                for (int j = 0; j < 2; j++) {
                    LogicalVariable varLeft = ((VariableReferenceExpression) (sfe.getArguments().get(j).getValue()))
                            .getVariableReference();
                    LogicalVariable varRight = ((VariableReferenceExpression) (sfe.getArguments().get((j + 1) % 2)
                            .getValue())).getVariableReference();
                    // We did not provide the merge of the partial ConstantExpression.
                    if (inLeftAll.contains(varLeft) && inRightAll.contains(varRight)) {
                        for (int i = 0; i < outLeftFields.size(); i++) {
                            if (varLeft.equals(outLeftFields.get(i)) && varRight.equals(outRightFields.get(i))) {
                                return updateAndGetRanges(outLeftFields, outRightFields, outBandRanges, i, ck, opRight);
                            }
                        }
                        outLeftFields.add(varLeft);
                        outRightFields.add(varRight);
                        outBandRanges.add(new Pair<ILogicalExpression, ILogicalExpression>(null, null));
                        return updateAndGetRanges(outLeftFields, outRightFields, outBandRanges,
                                outBandRanges.size() - 1, ck, opRight);
                    }
                }
            }
        }
        return SortMergeJoinType.NESTLOOP;
    }

    private static SortMergeJoinType updateAndGetRanges(List<LogicalVariable> outLeftFields,
            List<LogicalVariable> outRightFields, List<Pair<ILogicalExpression, ILogicalExpression>> bandRanges,
            int index, ComparisonKind ck, ILogicalExpression value) {
        switch (ck) {
            case GT:
            case GE: {
                // Add the ConstantExpression merge here in future.
                if (bandRanges.size() < index || null == bandRanges.get(index)) {
                    AlgebricksConfig.ALGEBRICKS_LOGGER.info("// Band condition left insert exception -- Condition for"
                            + value + ": " + ck);
                }
                bandRanges.get(index).first = value;
                break;
            }
            case LT:
            case LE: {
                if (bandRanges.size() < index || null == bandRanges.get(index)) {
                    AlgebricksConfig.ALGEBRICKS_LOGGER.info("// Band condition right insert exception -- Condition for"
                            + value + ": " + ck);
                }
                bandRanges.get(index).second = value;
                break;
            }
            default:
                break;
        }

        if (isBandRange(outLeftFields, outRightFields, bandRanges))
            return SortMergeJoinType.BAND;
        else if (isThetaRange(outLeftFields, outRightFields, bandRanges))
            return SortMergeJoinType.THETA;
        // Further for Metric and Skyline join.
        else
            return SortMergeJoinType.NESTLOOP;
    }

    private static boolean isBandRange(List<LogicalVariable> leftVars, List<LogicalVariable> rightVars,
            List<Pair<ILogicalExpression, ILogicalExpression>> bandRanges) {
        if (leftVars.size() != rightVars.size() || leftVars.size() != bandRanges.size())
            return false;
        for (int i = 0; i < bandRanges.size(); i++) {
            if (bandRanges.get(i).first != null && bandRanges.get(i).second != null)
                return true;
        }
        return false;
    }

    private static boolean isThetaRange(List<LogicalVariable> leftVars, List<LogicalVariable> rightVars,
            List<Pair<ILogicalExpression, ILogicalExpression>> bandRanges) {
        if (leftVars.size() != rightVars.size() || leftVars.size() != bandRanges.size())
            return false;
        for (int i = 0; i < bandRanges.size(); i++) {
            if (bandRanges.get(i).first != null || bandRanges.get(i).second != null)
                return true;
        }
        return false;
    }

    // Currently, we support the int and float/double and will make it general in the future.
    public static SortMergeJoinType getSortMergeJoinable(ILogicalOperator op, Collection<LogicalVariable> inLeftAll,
            Collection<LogicalVariable> inRightAll, List<LogicalVariable> outLeftFields,
            List<LogicalVariable> outRightFields, List<Pair<ILogicalExpression, ILogicalExpression>> outBandRanges) {
        // Three SortMergeJoinable operations: band, theta, metric and skyline. Currently just for band.
        ILogicalExpression e = ((AbstractBinaryJoinOperator) op).getCondition().getValue();
        switch (e.getExpressionTag()) {
            case FUNCTION_CALL: {
                // outBandRanges post process and cut off the band from the Select operator
                if (SortMergeJoinType.BAND == getSortMergeJoinable(e, inLeftAll, inRightAll, outLeftFields,
                        outRightFields, outBandRanges))
                    return SortMergeJoinType.BAND;
            }
            default:
                break;
        }
        return SortMergeJoinType.NESTLOOP;
    }

    public static boolean isDataSetCall(ILogicalExpression e) {
        if (((AbstractLogicalExpression) e).getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }
        AbstractFunctionCallExpression fe = (AbstractFunctionCallExpression) e;
        return AsterixBuiltinFunctions.isDatasetFunction(fe.getFunctionIdentifier());
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
            if (fid.equals(AsterixBuiltinFunctions.FIELD_ACCESS_BY_NAME)) {
                return true;
            }
        }
        return false;
    }

    public static boolean isAccessToFieldRecord(ILogicalExpression expr) {
        if (expr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
            AbstractFunctionCallExpression fc = (AbstractFunctionCallExpression) expr;
            FunctionIdentifier fid = fc.getFunctionIdentifier();
            if (fid.equals(AsterixBuiltinFunctions.FIELD_ACCESS_BY_INDEX)
                    || fid.equals(AsterixBuiltinFunctions.FIELD_ACCESS_BY_NAME)
                    || fid.equals(AsterixBuiltinFunctions.FIELD_ACCESS_NESTED)) {
                return true;
            }
        }
        return false;
    }

    public static Pair<String, String> getDatasetInfo(AbstractDataSourceOperator op) throws AlgebricksException {
        AqlSourceId srcId = (AqlSourceId) op.getDataSource().getId();
        return new Pair<String, String>(srcId.getDataverseName(), srcId.getDatasourceName());
    }

    public static Pair<String, String> getExternalDatasetInfo(UnnestMapOperator op) throws AlgebricksException {
        AbstractFunctionCallExpression unnestExpr = (AbstractFunctionCallExpression) op.getExpressionRef().getValue();
        String dataverseName = AccessMethodUtils.getStringConstant(unnestExpr.getArguments().get(0));
        String datasetName = AccessMethodUtils.getStringConstant(unnestExpr.getArguments().get(1));
        return new Pair<String, String>(dataverseName, datasetName);
    }

    private static List<FunctionIdentifier> fieldAccessFunctions = new ArrayList<FunctionIdentifier>();

    static {
        fieldAccessFunctions.add(AsterixBuiltinFunctions.GET_DATA);
        fieldAccessFunctions.add(AsterixBuiltinFunctions.GET_HANDLE);
        fieldAccessFunctions.add(AsterixBuiltinFunctions.TYPE_OF);
    }

}
