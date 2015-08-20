/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.algebricks.rewriter.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.BroadcastExpressionAnnotation;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.BroadcastExpressionAnnotation.BroadcastSide;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IExpressionAnnotation;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions.ComparisonKind;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.visitors.LogicalPropertiesVisitor;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.physical.AbstractJoinPOperator.JoinPartitioningType;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.physical.HybridHashJoinPOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.physical.InMemoryHashJoinPOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.physical.NLJoinPOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.ILogicalPropertiesVector;
import edu.uci.ics.hyracks.algebricks.core.config.AlgebricksConfig;

public class JoinUtils {

    public static void setJoinAlgorithmAndExchangeAlgo(AbstractBinaryJoinOperator op, IOptimizationContext context)
            throws AlgebricksException {
        List<LogicalVariable> sideLeft = new LinkedList<LogicalVariable>();
        List<LogicalVariable> sideRight = new LinkedList<LogicalVariable>();
        List<LogicalVariable> varsLeft = op.getInputs().get(0).getValue().getSchema();
        List<LogicalVariable> varsRight = op.getInputs().get(1).getValue().getSchema();
        if (isHashJoinCondition(op.getCondition().getValue(), varsLeft, varsRight, sideLeft, sideRight)) {
            BroadcastSide side = getBroadcastJoinSide(op.getCondition().getValue(), varsLeft, varsRight);
            if (side == null) {
                setHashJoinOp(op, JoinPartitioningType.PAIRWISE, sideLeft, sideRight, context);
            } else {
                switch (side) {
                    case RIGHT:
                        setHashJoinOp(op, JoinPartitioningType.BROADCAST, sideLeft, sideRight, context);
                        break;
                    case LEFT:
                        Mutable<ILogicalOperator> opRef0 = op.getInputs().get(0);
                        Mutable<ILogicalOperator> opRef1 = op.getInputs().get(1);
                        ILogicalOperator tmp = opRef0.getValue();
                        opRef0.setValue(opRef1.getValue());
                        opRef1.setValue(tmp);
                        setHashJoinOp(op, JoinPartitioningType.BROADCAST, sideRight, sideLeft, context);
                        break;
                    default:
                        setHashJoinOp(op, JoinPartitioningType.PAIRWISE, sideLeft, sideRight, context);
                }
            }
        } else {
            setNLJoinOp(op, context);
        }
    }

    private static void setNLJoinOp(AbstractBinaryJoinOperator op, IOptimizationContext context) {
        op.setPhysicalOperator(new NLJoinPOperator(op.getJoinKind(), JoinPartitioningType.BROADCAST, context
                .getPhysicalOptimizationConfig().getMaxRecordsPerFrame()));
    }

    private static void setHashJoinOp(AbstractBinaryJoinOperator op, JoinPartitioningType partitioningType,
            List<LogicalVariable> sideLeft, List<LogicalVariable> sideRight, IOptimizationContext context)
            throws AlgebricksException {
        op.setPhysicalOperator(new HybridHashJoinPOperator(op.getJoinKind(), partitioningType, sideLeft, sideRight,
                context.getPhysicalOptimizationConfig().getMaxFramesHybridHash(), context
                        .getPhysicalOptimizationConfig().getMaxFramesLeftInputHybridHash(), context
                        .getPhysicalOptimizationConfig().getMaxRecordsPerFrame(), context
                        .getPhysicalOptimizationConfig().getFudgeFactor()));
        if (partitioningType == JoinPartitioningType.BROADCAST) {
            hybridToInMemHashJoin(op, context);
        }
        // op.setPhysicalOperator(new
        // InMemoryHashJoinPOperator(op.getJoinKind(), partitioningType,
        // sideLeft, sideRight,
        // 1024 * 512));
    }

    private static void hybridToInMemHashJoin(AbstractBinaryJoinOperator op, IOptimizationContext context)
            throws AlgebricksException {
        ILogicalOperator opBuild = op.getInputs().get(1).getValue();
        LogicalPropertiesVisitor.computeLogicalPropertiesDFS(opBuild, context);
        ILogicalPropertiesVector v = context.getLogicalPropertiesVector(opBuild);
        AlgebricksConfig.ALGEBRICKS_LOGGER.fine("// HybridHashJoin inner branch -- Logical properties for " + opBuild
                + ": " + v + "\n");
        if (v != null) {
            int size2 = v.getMaxOutputFrames();
            HybridHashJoinPOperator hhj = (HybridHashJoinPOperator) op.getPhysicalOperator();
            if (size2 > 0 && size2 * hhj.getFudgeFactor() <= hhj.getMemSizeInFrames()) {
                AlgebricksConfig.ALGEBRICKS_LOGGER.fine("// HybridHashJoin inner branch " + opBuild
                        + " fits in memory\n");
                // maintains the local properties on the probe side
                op.setPhysicalOperator(new InMemoryHashJoinPOperator(hhj.getKind(), hhj.getPartitioningType(), hhj
                        .getKeysLeftBranch(), hhj.getKeysRightBranch(), v.getNumberOfTuples() * 2));
            }
        }

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
                        if (!isHashJoinCondition(a.getValue(), inLeftAll, inRightAll, outLeftFields,
                                outRightFields)) {
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
            default: {
                return false;
            }
        }
    }

    private static BroadcastSide getBroadcastJoinSide(ILogicalExpression e, List<LogicalVariable> varsLeft,
            List<LogicalVariable> varsRight) {
        if (e.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return null;
        }
        AbstractFunctionCallExpression fexp = (AbstractFunctionCallExpression) e;
        IExpressionAnnotation ann = fexp.getAnnotations().get(BroadcastExpressionAnnotation.BROADCAST_ANNOTATION_KEY);
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
        ArrayList<LogicalVariable> vars = new ArrayList<LogicalVariable>();
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
