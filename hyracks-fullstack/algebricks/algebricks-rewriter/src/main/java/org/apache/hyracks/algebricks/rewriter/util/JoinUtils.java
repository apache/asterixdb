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
package org.apache.hyracks.algebricks.rewriter.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.BroadcastExpressionAnnotation;
import org.apache.hyracks.algebricks.core.algebra.expressions.BroadcastExpressionAnnotation.BroadcastSide;
import org.apache.hyracks.algebricks.core.algebra.expressions.IExpressionAnnotation;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions.ComparisonKind;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.LogicalPropertiesVisitor;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.AbstractJoinPOperator.JoinPartitioningType;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.HybridHashJoinPOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.InMemoryHashJoinPOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.NestedLoopJoinPOperator;
import org.apache.hyracks.algebricks.core.algebra.properties.ILogicalPropertiesVector;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;
import org.apache.hyracks.algebricks.core.config.AlgebricksConfig;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.api.exceptions.Warning;

public class JoinUtils {
    private JoinUtils() {
    }

    public static void setJoinAlgorithmAndExchangeAlgo(AbstractBinaryJoinOperator op, boolean topLevelOp,
            IOptimizationContext context) {
        if (!topLevelOp) {
            throw new IllegalStateException("Micro operator not implemented for: " + op.getOperatorTag());
        }
        List<LogicalVariable> sideLeft = new LinkedList<>();
        List<LogicalVariable> sideRight = new LinkedList<>();
        List<LogicalVariable> varsLeft = op.getInputs().get(0).getValue().getSchema();
        List<LogicalVariable> varsRight = op.getInputs().get(1).getValue().getSchema();
        ILogicalExpression conditionExpr = op.getCondition().getValue();
        if (isHashJoinCondition(conditionExpr, varsLeft, varsRight, sideLeft, sideRight)) {
            BroadcastSide side = getBroadcastJoinSide(conditionExpr, varsLeft, varsRight);
            if (side == null) {
                setHashJoinOp(op, JoinPartitioningType.PAIRWISE, sideLeft, sideRight, context);
            } else {
                switch (side) {
                    case RIGHT:
                        setHashJoinOp(op, JoinPartitioningType.BROADCAST, sideLeft, sideRight, context);
                        break;
                    case LEFT:
                        if (op.getJoinKind() == AbstractBinaryJoinOperator.JoinKind.INNER) {
                            Mutable<ILogicalOperator> opRef0 = op.getInputs().get(0);
                            Mutable<ILogicalOperator> opRef1 = op.getInputs().get(1);
                            ILogicalOperator tmp = opRef0.getValue();
                            opRef0.setValue(opRef1.getValue());
                            opRef1.setValue(tmp);
                            setHashJoinOp(op, JoinPartitioningType.BROADCAST, sideRight, sideLeft, context);
                        } else {
                            setHashJoinOp(op, JoinPartitioningType.PAIRWISE, sideLeft, sideRight, context);
                        }
                        break;
                    default:
                        // This should never happen
                        throw new IllegalStateException(side.toString());
                }
            }
        } else {
            warnIfCrossProduct(conditionExpr, op.getSourceLocation(), context);
            setNestedLoopJoinOp(op);
        }
    }

    private static void setNestedLoopJoinOp(AbstractBinaryJoinOperator op) {
        op.setPhysicalOperator(new NestedLoopJoinPOperator(op.getJoinKind(), JoinPartitioningType.BROADCAST));
    }

    private static void setHashJoinOp(AbstractBinaryJoinOperator op, JoinPartitioningType partitioningType,
            List<LogicalVariable> sideLeft, List<LogicalVariable> sideRight, IOptimizationContext context) {
        op.setPhysicalOperator(new HybridHashJoinPOperator(op.getJoinKind(), partitioningType, sideLeft, sideRight,
                context.getPhysicalOptimizationConfig().getMaxFramesForJoinLeftInput(),
                context.getPhysicalOptimizationConfig().getMaxRecordsPerFrame(),
                context.getPhysicalOptimizationConfig().getFudgeFactor()));
    }

    public static boolean hybridToInMemHashJoin(AbstractBinaryJoinOperator op, IOptimizationContext context)
            throws AlgebricksException {
        HybridHashJoinPOperator hhj = (HybridHashJoinPOperator) op.getPhysicalOperator();
        if (hhj.getPartitioningType() != JoinPartitioningType.BROADCAST) {
            return false;
        }
        ILogicalOperator opBuild = op.getInputs().get(1).getValue();
        LogicalPropertiesVisitor.computeLogicalPropertiesDFS(opBuild, context);
        ILogicalPropertiesVector v = context.getLogicalPropertiesVector(opBuild);
        boolean loggerTraceEnabled = AlgebricksConfig.ALGEBRICKS_LOGGER.isTraceEnabled();
        if (loggerTraceEnabled) {
            AlgebricksConfig.ALGEBRICKS_LOGGER.trace("// HybridHashJoin inner branch -- Logical properties for "
                    + opBuild.getOperatorTag() + ": " + v + "\n");
        }
        if (v != null) {
            int size2 = v.getMaxOutputFrames();
            int hhjMemSizeInFrames = hhj.getLocalMemoryRequirements().getMemoryBudgetInFrames();
            if (size2 > 0 && size2 * hhj.getFudgeFactor() <= hhjMemSizeInFrames) {
                if (loggerTraceEnabled) {
                    AlgebricksConfig.ALGEBRICKS_LOGGER
                            .trace("// HybridHashJoin inner branch " + opBuild.getOperatorTag() + " fits in memory\n");
                }
                // maintains the local properties on the probe side
                op.setPhysicalOperator(new InMemoryHashJoinPOperator(hhj.getKind(), hhj.getPartitioningType(),
                        hhj.getKeysLeftBranch(), hhj.getKeysRightBranch(), v.getNumberOfTuples() * 2));
                return true;
            }
        }
        return false;
    }

    private static boolean isHashJoinCondition(ILogicalExpression e, Collection<LogicalVariable> inLeftAll,
            Collection<LogicalVariable> inRightAll, Collection<LogicalVariable> outLeftFields,
            Collection<LogicalVariable> outRightFields) {
        switch (e.getExpressionTag()) {
            case FUNCTION_CALL: {
                AbstractFunctionCallExpression fexp = (AbstractFunctionCallExpression) e;
                FunctionIdentifier fi = fexp.getFunctionIdentifier();
                if (fi.equals(AlgebricksBuiltinFunctions.AND)) {
                    for (Mutable<ILogicalExpression> a : fexp.getArguments()) {
                        if (!isHashJoinCondition(a.getValue(), inLeftAll, inRightAll, outLeftFields, outRightFields)) {
                            return false;
                        }
                    }
                    return true;
                } else {
                    ComparisonKind ck = AlgebricksBuiltinFunctions.getComparisonType(fi);
                    if (ck != ComparisonKind.EQ) {
                        return false;
                    }
                    ILogicalExpression opLeft = fexp.getArguments().get(0).getValue();
                    ILogicalExpression opRight = fexp.getArguments().get(1).getValue();
                    if (opLeft.getExpressionTag() != LogicalExpressionTag.VARIABLE
                            || opRight.getExpressionTag() != LogicalExpressionTag.VARIABLE) {
                        return false;
                    }
                    LogicalVariable var1 = ((VariableReferenceExpression) opLeft).getVariableReference();
                    if (inLeftAll.contains(var1) && !outLeftFields.contains(var1)) {
                        outLeftFields.add(var1);
                    } else if (inRightAll.contains(var1) && !outRightFields.contains(var1)) {
                        outRightFields.add(var1);
                    } else {
                        return false;
                    }
                    LogicalVariable var2 = ((VariableReferenceExpression) opRight).getVariableReference();
                    if (inLeftAll.contains(var2) && !outLeftFields.contains(var2)) {
                        outLeftFields.add(var2);
                    } else if (inRightAll.contains(var2) && !outRightFields.contains(var2)) {
                        outRightFields.add(var2);
                    } else {
                        return false;
                    }
                    return true;
                }
            }
            default:
                return false;
        }
    }

    private static BroadcastSide getBroadcastJoinSide(ILogicalExpression e, List<LogicalVariable> varsLeft,
            List<LogicalVariable> varsRight) {
        if (e.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return null;
        }
        AbstractFunctionCallExpression fexp = (AbstractFunctionCallExpression) e;
        FunctionIdentifier fi = fexp.getFunctionIdentifier();
        if (fi.equals(AlgebricksBuiltinFunctions.AND)) {
            BroadcastSide fBcastSide = null;
            for (Mutable<ILogicalExpression> a : fexp.getArguments()) {
                BroadcastSide aBcastSide = getBroadcastJoinSide(a.getValue(), varsLeft, varsRight);
                if (fBcastSide == null) {
                    fBcastSide = aBcastSide;
                } else if (aBcastSide != null && !aBcastSide.equals(fBcastSide)) {
                    return null;
                }
            }
            return fBcastSide;
        } else {
            IExpressionAnnotation ann =
                    fexp.getAnnotations().get(BroadcastExpressionAnnotation.BROADCAST_ANNOTATION_KEY);
            if (ann == null) {
                return null;
            }
            BroadcastSide side = (BroadcastSide) ann.getObject();
            if (side == null) {
                return null;
            }
            int i;
            switch (side) {
                case LEFT:
                    i = 0;
                    break;
                case RIGHT:
                    i = 1;
                    break;
                default:
                    return null;
            }
            ArrayList<LogicalVariable> vars = new ArrayList<>();
            fexp.getArguments().get(i).getValue().getUsedVariables(vars);
            if (varsLeft.containsAll(vars)) {
                return BroadcastSide.LEFT;
            } else if (varsRight.containsAll(vars)) {
                return BroadcastSide.RIGHT;
            } else {
                return null;
            }
        }
    }

    private static void warnIfCrossProduct(ILogicalExpression conditionExpr, SourceLocation sourceLoc,
            IOptimizationContext context) {
        if (OperatorPropertiesUtil.isAlwaysTrueCond(conditionExpr) && sourceLoc != null) {
            IWarningCollector warningCollector = context.getWarningCollector();
            if (warningCollector.shouldWarn()) {
                warningCollector.warn(Warning.forHyracks(sourceLoc, ErrorCode.CROSS_PRODUCT_JOIN));
            }
        }
    }
}
